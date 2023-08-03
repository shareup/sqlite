import Combine
import Foundation
import GRDB
import os.log
import SQLite3
import Synchronized

public final class SQLiteDatabase: @unchecked Sendable {
    public static let suspendNotification = GRDB.Database.suspendNotification
    public static let resumeNotification = GRDB.Database.resumeNotification

    public let path: String
    public let sqliteVersion: String

    private let database: Database
    private let triggerObservers = PassthroughSubject<Void, Never>()
    private var changeNotifier: CrossProcessChangeNotifier!
    private var notificationSubscriptions = Set<AnyCancellable>()

    private let publisherQueue: DispatchQueue = .init(
        label: "app.shareup.sqlite.publisher-queue",
        qos: .default,
        autoreleaseFrequency: .workItem,
        target: DispatchQueue.global()
    )

    public static func makeShared(
        path: String,
        busyTimeout: TimeInterval = 5
    ) throws -> SQLiteDatabase {
        guard path != ":memory:", let url = URL(string: path)
        else { throw SQLiteError.SQLITE_IOERR }

        let coordinator = NSFileCoordinator(filePresenter: nil)
        var fileCoordinatorError: NSError?

        var database: SQLiteDatabase?
        var databaseError: Error?

        coordinator.coordinate(
            writingItemAt: url,
            options: .forMerging,
            error: &fileCoordinatorError
        ) { url in
            do {
                database = try SQLiteDatabase(
                    path: url.path,
                    busyTimeout: busyTimeout
                )
            } catch {
                databaseError = error
            }
        }

        guard let db = database,
              fileCoordinatorError == nil,
              databaseError == nil
        else {
            if let error = (fileCoordinatorError ?? databaseError) {
                try rethrowAsSQLiteError(error)
            } else {
                throw SQLiteError.SQLITE_CANTOPEN
            }
        }

        return db
    }

    public init(path: String = ":memory:", busyTimeout: TimeInterval = 5) throws {
        database = try Self.open(at: path, busyTimeout: busyTimeout)
        self.path = path
        let sqliteVersion = try Self.getSQLiteVersion(database)
        self.sqliteVersion = sqliteVersion.description
        changeNotifier = CrossProcessChangeNotifier(
            databasePath: path,
            onRemoteChange: { [weak self] in
                self?.triggerObservers.send()
            }
        )

        try checkIsSQLiteVersionSupported(sqliteVersion)
        precondition(enforcesForeignKeyConstraints)

        registerForAppNotifications()
        changeNotifier.start()
    }

    func resume() {
        NotificationCenter.default.post(
            name: Self.resumeNotification,
            object: nil
        )
        touch()
    }

    func suspend() {
        NotificationCenter.default.post(
            name: Self.suspendNotification,
            object: nil
        )
    }
}

// MARK: - Asynchronous queries - deprecated

public extension SQLiteDatabase {
    @available(*, deprecated, message: "Use Swift Concurrency")
    func inTransactionPublisher<T>(
        _ block: @escaping (DatabaseProxy) throws -> T
    ) -> AnyPublisher<T, Error> {
        database
            .writer
            .writePublisher(receiveOn: publisherQueue) { db in
                var result: T!
                try db.inSavepoint {
                    result = try block(.init(db))
                    return .commit
                }
                return result
            }
            .mapToSQLiteError()
            .eraseToAnyPublisher()
    }

    @available(*, deprecated, message: "Use Swift Concurrency")
    func writePublisher(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<Void, Error> {
        database
            .writer
            .writePublisher(receiveOn: publisherQueue) { db in
                let statement = try db.cachedStatement(sql: sql)
                try statement.execute(
                    arguments: arguments.isEmpty
                        ? nil
                        : arguments.statementArguments
                )
            }
            .mapToSQLiteError(sql: sql)
            .eraseToAnyPublisher()
    }

    @available(*, deprecated, message: "Use Swift Concurrency")
    func readPublisher(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<[SQLiteRow], Error> {
        database
            .reader
            .readPublisher(receiveOn: publisherQueue) { db in
                let statement = try db.cachedStatement(sql: sql)
                return try Row.fetchAll(
                    statement,
                    arguments: arguments.isEmpty
                        ? nil
                        : arguments.statementArguments
                ).compactMap(SQLiteRow.init(row:))
            }
            .mapToSQLiteError(sql: sql)
            .eraseToAnyPublisher()
    }

    @available(*, deprecated, message: "Use Swift Concurrency")
    func readPublisher<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<[T], Error> {
        readPublisher(sql, arguments: arguments)
            .tryMap { try $0.map { try T(row: $0) } }
            .mapToSQLiteError(sql: sql)
            .eraseToAnyPublisher()
    }
}

// MARK: - Asynchronous queries

public extension SQLiteDatabase {
    func inTransaction<T>(
        _ block: @escaping (DatabaseProxy) throws -> T
    ) async throws -> T {
        do {
            return try await database.writer.write { db in
                var result: T!
                try db.inSavepoint {
                    result = try block(.init(db))
                    return .commit
                }
                return result
            }
        } catch {
            os_log(
                "in-transaction: error=%s",
                log: log,
                type: .error,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }

    func write(_ sql: SQL, arguments: SQLiteArguments = [:]) async throws {
        do {
            try await database.writer.write { db in
                let statement = try db.cachedStatement(sql: sql)
                try statement.execute(
                    arguments: arguments.isEmpty
                        ? nil
                        : arguments.statementArguments
                )
            }
        } catch {
            os_log(
                "write: sql=%s error=%s",
                log: log,
                type: .error,
                sql,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }

    func read(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) async throws -> [SQLiteRow] {
        do {
            return try await database.reader.read { db in
                let statement = try db.cachedStatement(sql: sql)
                return try Row.fetchAll(
                    statement,
                    arguments: arguments.isEmpty
                        ? nil
                        : arguments.statementArguments
                ).compactMap(SQLiteRow.init(row:))
            }
        } catch {
            os_log(
                "read: sql=%s error=%s",
                log: log,
                type: .error,
                sql,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }

    func read<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) async throws -> [T] {
        do {
            return try await database.reader.read { db in
                let statement = try db.cachedStatement(sql: sql)
                return try Row.fetchAll(
                    statement,
                    arguments: arguments.isEmpty
                        ? nil
                        : arguments.statementArguments
                )
                .compactMap(SQLiteRow.init(row:))
                .map(T.init)
            }
        } catch {
            os_log(
                "read: sql=%s error=%s",
                log: log,
                type: .error,
                sql,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }

    @discardableResult
    func execute(raw sql: SQL) async throws -> [SQLiteRow] {
        do {
            return try await database.writer.write { db in
                try Row.fetchAll(db, sql: sql)
                    .compactMap(SQLiteRow.init(row:))
            }
        } catch {
            os_log(
                "execute: sql=%s error=%s",
                log: log,
                type: .error,
                sql,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }
}

// MARK: - Synchronous queries

public extension SQLiteDatabase {
    func inTransaction<T>(
        _ block: (DatabaseProxy) throws -> T
    ) throws -> T {
        do {
            return try database.writer.write { db in
                var result: T!
                try db.inSavepoint {
                    result = try block(.init(db))
                    return .commit
                }
                return result
            }
        } catch {
            os_log(
                "in-transaction: error=%s",
                log: log,
                type: .error,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }

    func write(_ sql: SQL, arguments: SQLiteArguments = [:]) throws {
        try database.write(sql, arguments: arguments)
    }

    func read(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws -> [SQLiteRow] {
        try database.read(sql, arguments: arguments)
    }

    func read<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws -> [T] {
        try database.read(sql, arguments: arguments)
            .map(T.init)
    }

    @discardableResult
    func execute(raw sql: SQL) throws -> [SQLiteRow] {
        do {
            return try database.writer.write { db in
                let statement = try db.makeStatement(sql: sql)
                return try Row.fetchAll(statement)
                    .compactMap(SQLiteRow.init(row:))
            }
        } catch {
            os_log(
                "execute: sql=%s error=%s",
                log: log,
                type: .error,
                sql,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }
}

// MARK: - Tables and columns

public extension SQLiteDatabase {
    func tables() throws -> [String] {
        let sql = "SELECT * FROM sqlite_master WHERE type='table';"
        return try execute(raw: sql)
            .compactMap { $0["tbl_name"]?.stringValue }
    }

    func columns(in table: String) throws -> [String] {
        let sql =
            """
            SELECT DISTINCT ti.name AS name, ti.pk AS pk
                FROM sqlite_master AS m, pragma_table_info(m.name) as ti
                WHERE m.type='table'
                AND m.name='\(table)';
            """

        return try execute(raw: sql)
            .compactMap { $0["name"]?.stringValue }
    }
}

// MARK: - Combine Publishers observing SQL queries

public extension SQLiteDatabase {
    func publisher(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<[SQLiteRow], Error> {
        database.observe(
            sql,
            arguments: arguments,
            trigger: triggerObservers.eraseToAnyPublisher(),
            queue: publisherQueue
        )
    }

    // Swift favors type inference and, consequently, does not allow specializing functions at
    // the call site.
    // This means that combining multiple `Combine.Publisher` together can be frustrating
    // because
    // Swift can't infer the type. Adding this function that includes the generic type means we
    // don't
    // need to specify the type at the call site using `as Array<T>`.
    // https://forums.swift.org/t/compiler-cannot-infer-the-type-of-a-generic-method-cannot-specialize-a-non-generic-definition/10294
    func publisher<T: SQLiteTransformable>(
        _: T.Type,
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<[T], Error> {
        publisher(
            sql,
            arguments: arguments
        ) as AnyPublisher<[T], Error>
    }

    func publisher<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<[T], Error> {
        publisher(sql, arguments: arguments)
            .tryMap { try $0.map { try T(row: $0) } }
            .mapToSQLiteError(sql: sql)
            .eraseToAnyPublisher()
    }
}

// MARK: - Trigger updates for observers

public extension SQLiteDatabase {
    func touch() {
        triggerObservers.send()
    }
}

// MARK: - App Notifications

#if canImport(UIKit)

    import UIKit

    private extension SQLiteDatabase {
        private func registerForAppNotifications() {
            let center = NotificationCenter.default

            center
                .publisher(for: UIApplication.didBecomeActiveNotification)
                .sink { [weak self] _ in
                    guard let self else { return }
                    resume()
                    changeNotifier.start()
                }
                .store(in: &notificationSubscriptions)

            center
                .publisher(for: UIApplication.didEnterBackgroundNotification)
                .sink { [weak self] _ in
                    guard let self else { return }
                    changeNotifier.stop()
                    suspend()
                }
                .store(in: &notificationSubscriptions)
        }
    }

#else

    private extension SQLiteDatabase {
        private func registerForAppNotifications() {}
    }

#endif

// MARK: - Equatable

extension SQLiteDatabase: Equatable {
    public static func == (lhs: SQLiteDatabase, rhs: SQLiteDatabase) -> Bool {
        ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
    }
}

// MARK: - Compile-time SQLite options

public extension SQLiteDatabase {
    var supportsJSON: Bool {
        let isEnabled = isCompileOptionEnabled("SQLITE_ENABLE_JSON1")

        guard let version = try? SQLiteVersion(self)
        else { return isEnabled }

        // https://sqlite.org/compile.html#enable_json1
        return version >= SQLiteVersion(major: 3, minor: 38, patch: 0) || isEnabled
    }

    var supportsPreupdateHook: Bool {
        isCompileOptionEnabled("SQLITE_ENABLE_PREUPDATE_HOOK")
    }

    func isCompileOptionEnabled(_ name: String) -> Bool {
        sqlite3_compileoption_used(name) == Int32(1)
    }
}

// MARK: - Pragmas

public extension SQLiteDatabase {
    var userVersion: Int {
        get {
            do {
                guard let result = try execute(raw: "PRAGMA user_version;").first
                else { return 0 }
                return result["user_version"]?.intValue ?? 0
            } catch {
                assertionFailure("Could not get user_version: \(error)")
                return 0
            }
        }
        set {
            do {
                try execute(raw: "PRAGMA user_version = \(newValue);")
            } catch {
                assertionFailure("Could not set user_version to \(newValue): \(error)")
            }
        }
    }

    var enforcesForeignKeyConstraints: Bool {
        get {
            do {
                guard let result = try execute(raw: "PRAGMA foreign_keys;").first
                else { return false }
                return result["foreign_keys"]?.boolValue ?? false
            } catch {
                assertionFailure("Could not get foreign_keys: \(error)")
                return false
            }
        }
        set {
            do {
                try database.writer.barrierWriteWithoutTransaction { db in
                    let sql = "PRAGMA foreign_keys = \(newValue ? "ON" : "OFF");"
                    let statement = try db.makeStatement(sql: sql)
                    try statement.execute()
                }
            } catch {
                assertionFailure(
                    "Could not set foreign_keys to \(newValue): \(error)"
                )
            }
        }
    }
}

// MARK: - Vacuuming

public extension SQLiteDatabase {
    enum AutoVacuumMode: Int {
        case none = 0
        case incremental = 2
        case full = 1

        fileprivate init?(_ rawValue: Int?) {
            switch rawValue {
            case .some(0): self = .none
            case .some(2): self = .incremental
            case .some(1): self = .full
            default: return nil
            }
        }
    }

    var autoVacuumMode: AutoVacuumMode {
        get {
            do {
                guard let result = try execute(raw: "PRAGMA auto_vacuum;").first
                else { return .none }
                return AutoVacuumMode(result["auto_vacuum"]?.intValue) ?? .none
            } catch {
                assertionFailure("Could not get auto_vacuum: \(error))")
                return .none
            }
        }
        set {
            do { try execute(raw: "PRAGMA auto_vacuum = \(newValue.rawValue);") }
            catch { assertionFailure("Could not set auto_vacuum to \(newValue): \(error)") }
        }
    }

    func incrementalVacuum(_ pages: Int? = nil) throws {
        let sql: SQL
        if let pages {
            sql = "PRAGMA incremental_vacuum(\(pages));"
        } else {
            sql = "PRAGMA incremental_vacuum;"
        }
        try execute(raw: sql)
    }

    func vacuum() throws {
        do {
            try database.writer.vacuum()
        } catch {
            os_log(
                "vacuum: error=%s",
                log: log,
                type: .error,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }
}

extension SQLiteDatabase {
    private func checkIsSQLiteVersionSupported(
        _ version: SQLiteVersion
    ) throws {
        guard version.isSupported else {
            os_log(
                "version: error=unsupported major=%lld minor=%lld patch=%lld",
                log: log,
                type: .error,
                version.major,
                version.minor,
                version.patch
            )
            throw SQLiteError.SQLITE_ERROR
        }
    }
}

private extension SQLiteDatabase {
    class func open(
        at path: String,
        busyTimeout: TimeInterval
    ) throws -> Database {
        var config = Configuration()
        config.busyMode = .timeout(busyTimeout)
        config.observesSuspensionNotifications = true

        guard path != ":memory:" else {
            do {
                let queue = try DatabaseQueue(
                    path: path,
                    configuration: config
                )
                return .queue(queue)
            } catch {
                try rethrowAsSQLiteError(error)
            }
        }

        config.prepareDatabase { db in
            if !db.configuration.readonly {
                var flag: CInt = 1
                let code = withUnsafeMutablePointer(to: &flag) { _flag in
                    sqlite3_file_control(
                        db.sqliteConnection,
                        nil,
                        SQLITE_FCNTL_PERSIST_WAL,
                        _flag
                    )
                }
                guard code == SQLITE_OK else {
                    throw SQLiteError.SQLITE_CANTOPEN
                }
            }
        }

        do {
            let pool = try DatabasePool(path: path, configuration: config)
            return .pool(pool)
        } catch {
            try rethrowAsSQLiteError(error)
        }
    }

    class func getSQLiteVersion(_ db: Database) throws -> SQLiteVersion {
        let rows = try db.read(SQLiteVersion.selectVersion)
        return try SQLiteVersion(rows: rows)
    }
}

public struct DatabaseProxy {
    private let db: GRDB.Database

    init(_ database: GRDB.Database) {
        db = database
    }

    public func read(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws -> [SQLiteRow] {
        try db.read(sql, arguments: arguments)
    }

    public func read<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws -> [T] {
        try read(sql, arguments: arguments)
            .map(T.init(row:))
    }

    public func write(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws {
        try db.write(sql, arguments: arguments)
    }

    @discardableResult
    public func execute(raw: SQL) throws -> [SQLiteRow] {
        try db.execute(raw: raw)
    }
}

private enum Database {
    case pool(DatabasePool)
    case queue(DatabaseQueue)

    var reader: AnyDatabaseReader {
        switch self {
        case let .pool(pool): return AnyDatabaseReader(pool)
        case let .queue(queue): return AnyDatabaseReader(queue)
        }
    }

    var writer: AnyDatabaseWriter {
        switch self {
        case let .pool(pool): return AnyDatabaseWriter(pool)
        case let .queue(queue): return AnyDatabaseWriter(queue)
        }
    }

    func read(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws -> [SQLiteRow] {
        do {
            return try reader.read { db in
                try db.read(sql, arguments: arguments)
            }
        } catch {
            try rethrowAsSQLiteError(error)
        }
    }

    func write(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws {
        do {
            try writer.write { db in
                try db.write(sql, arguments: arguments)
            }
        } catch {
            try rethrowAsSQLiteError(error)
        }
    }

    func observe(
        _ sql: SQL,
        arguments: SQLiteArguments,
        trigger: AnyPublisher<Void, Never>,
        queue: DispatchQueue
    ) -> AnyPublisher<[SQLiteRow], Error> {
        let request = SQLRequest(
            sql: sql,
            arguments: arguments.statementArguments
        )

        let currentValuePub = Future<[SQLiteRow], Error> { result in
            do { result(.success(try read(sql, arguments: arguments))) }
            catch { result(.failure(error)) }
        }.receive(on: queue)

        let observationPub = DatabaseRegionObservation(tracking: [request])
            .publisher(in: writer)
            .receive(on: queue)
            .tryMap { _ in try read(sql, arguments: arguments) }

        let triggerPub = trigger
            .tryMap { _ in try read(sql, arguments: arguments) }
            .receive(on: queue)

        return Publishers.Merge3(currentValuePub, observationPub, triggerPub)
            .mapToSQLiteError(sql: sql)
            .eraseToAnyPublisher()
    }
}

private extension GRDB.Database {
    func read(
        _ sql: SQL,
        arguments: SQLiteArguments
    ) throws -> [SQLiteRow] {
        do {
            let statement = try cachedStatement(sql: sql)
            return try Row.fetchAll(
                statement,
                arguments: arguments.isEmpty
                    ? nil
                    : arguments.statementArguments
            ).compactMap(SQLiteRow.init(row:))
        } catch {
            os_log(
                "read: sql=%s error=%s",
                log: log,
                type: .error,
                sql,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }

    func write(
        _ sql: SQL,
        arguments: SQLiteArguments
    ) throws {
        do {
            let statement = try cachedStatement(sql: sql)
            try statement.execute(
                arguments: arguments.isEmpty
                    ? nil
                    : arguments.statementArguments
            )
        } catch {
            os_log(
                "write: sql=%s error=%s",
                log: log,
                type: .error,
                sql,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }

    @discardableResult
    func execute(raw sql: SQL) throws -> [SQLiteRow] {
        do {
            return try Row.fetchAll(self, sql: sql)
                .compactMap(SQLiteRow.init(row:))
        } catch {
            os_log(
                "execute: sql=%s error=%s",
                log: log,
                type: .error,
                sql,
                String(describing: error)
            )
            try rethrowAsSQLiteError(error)
        }
    }
}
