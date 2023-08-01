import Combine
import Foundation
import SQLite3
import GRDB
import Synchronized

public final class SQLiteDatabase {
    public static let suspendNotification = GRDB.Database.suspendNotification
    public static let resumeNotification = GRDB.Database.resumeNotification
    
    public let path: String
    internal let sqliteVersion: SQLiteVersion
    private let database: Database
    
    private let queue: OperationQueue = {
        let queue = OperationQueue()
        queue.name = "app.shareup.sqlite.publisher-queue"
        queue.maxConcurrentOperationCount = 4
        queue.underlyingQueue = DispatchQueue.global()
        return queue
    }()


    public static func makeShared(
        path: String,
        busyTimeout seconds: TimeInterval = 0
    ) throws -> SQLiteDatabase {
        guard path != ":memory:", let url = URL(string: path)
        else { throw SQLiteError.onInvalidPath(path) }

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
                database = try SQLiteDatabase(path: url.path)
            } catch {
                databaseError = error
            }
        }

        guard let db = database,
              fileCoordinatorError == nil,
              databaseError == nil
        else {
            let error = String(describing: fileCoordinatorError ?? databaseError)
            throw SQLiteError.onOpenSharedDatabase(path, error)
        }
        
        return db
    }

    public init(path: String = ":memory:") throws {
        database = try Self.open(at: path, busyTimeout: 1)
        self.path = path
        sqliteVersion = try Self.getSQLiteVersion(database)
        try checkIsSQLiteVersionSupported()
        precondition(isForeignKeySupportEnabled)
    }
    
    public func resume() {
        NotificationCenter.default.post(
            name: Self.resumeNotification,
            object: nil
        )
    }
    
    public func suspend() {
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
    ) -> AnyPublisher<T, SQLiteError> {
        database
            .writer
            .writePublisher(receiveOn: queue) { db in
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
    ) -> AnyPublisher<Void, SQLiteError> {
        database
            .writer
            .writePublisher(receiveOn: queue) { db in
                let statement = try db.cachedStatement(sql: sql)
                try statement.execute(
                    arguments: arguments.isEmpty
                        ? nil
                        : arguments.statementArguments
                )
            }
            .mapToSQLiteError()
            .eraseToAnyPublisher()
    }

    @available(*, deprecated, message: "Use Swift Concurrency")
    func readPublisher(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<[SQLiteRow], SQLiteError> {
        database
            .reader
            .readPublisher(receiveOn: queue) { db in
                let statement = try db.cachedStatement(sql: sql)
                return try Row.fetchAll(
                    statement,
                    arguments: arguments.isEmpty
                        ? nil
                        : arguments.statementArguments
                ).compactMap(SQLiteRow.init(row:))
            }
            .mapToSQLiteError()
            .eraseToAnyPublisher()
    }

    @available(*, deprecated, message: "Use Swift Concurrency")
    func readPublisher<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<[T], SQLiteError> {
        readPublisher(sql, arguments: arguments)
            .tryMap { try $0.map { try T(row: $0) } }
            .mapToSQLiteError()
            .eraseToAnyPublisher()
    }
}

// MARK: - Asynchronous queries

public extension SQLiteDatabase {
    func inTransaction<T>(
        _ block: @escaping (DatabaseProxy) throws -> T
    ) async throws -> T {
        try await database.writer.write { db in
            var result: T!
            try db.inSavepoint {
                result = try block(.init(db))
                return .commit
            }
            return result
        }
    }

    func write(_ sql: SQL, arguments: SQLiteArguments = [:]) async throws {
        try await database.writer.write { db in
            let statement = try db.cachedStatement(sql: sql)
            try statement.execute(
                arguments: arguments.isEmpty
                    ? nil
                    : arguments.statementArguments
            )
        }
    }

    func read(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) async throws -> [SQLiteRow] {
        try await database.reader.read { db in
            let statement = try db.cachedStatement(sql: sql)
            return try Row.fetchAll(
                statement,
                arguments: arguments.isEmpty
                    ? nil
                    : arguments.statementArguments
            ).compactMap(SQLiteRow.init(row:))
        }
    }

    func read<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) async throws -> [T] {
        try await database.reader.read { db in
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
    }

    @discardableResult
    func execute(raw sql: SQL) async throws -> [SQLiteRow] {
        try await database.writer.write { db in
            return try Row.fetchAll(db, sql: sql)
                .compactMap(SQLiteRow.init(row:))
        }
    }
}

// MARK: - Synchronous queries

public extension SQLiteDatabase {
    func inTransaction<T>(
        _ block: (DatabaseProxy) throws -> T
    ) throws -> T {
        try database.writer.write { db in
            var result: T!
            try db.inSavepoint {
                result = try block(.init(db))
                return .commit
            }
            return result
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
        try database.writer.write { db in
            let statement = try db.makeStatement(sql: sql)
            return try Row.fetchAll(statement)
                .compactMap(SQLiteRow.init(row:))
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
        arguments: SQLiteArguments = [:],
        tables: [String] = []
    ) -> AnyPublisher<[SQLiteRow], SQLiteError> {
        database.observe(sql, arguments: arguments, queue: queue)
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
        arguments: SQLiteArguments = [:],
        tables: [String] = []
    ) -> AnyPublisher<[T], SQLiteError> {
        publisher(
            sql,
            arguments: arguments
        ) as AnyPublisher<[T], SQLiteError>
    }

    func publisher<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:],
        tables: [String] = []
    ) -> AnyPublisher<[T], SQLiteError> {
        publisher(sql, arguments: arguments)
            .tryMap { try $0.map { try T(row: $0) } }
            .mapToSQLiteError()
            .eraseToAnyPublisher()
    }
}

// MARK: - Trigger updates for observers

public extension SQLiteDatabase {
    func touch(_ tableName: String) {
        touch([tableName])
    }

    func touch(_ tableNames: [String] = []) {
        // TODO: Notify this process when another one changes.
    }
}

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

        guard let version = try? SQLiteVersion(self) else { return isEnabled }

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

    var isForeignKeySupportEnabled: Bool {
        do {
            guard let result = try execute(raw: "PRAGMA foreign_keys;").first
            else { return false }
            return result["foreign_keys"]?.boolValue ?? false
        } catch {
            assertionFailure("Could not get foreign_keys: \(error)")
            return false
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
        try database.writer.vacuum()
    }
}

extension SQLiteDatabase {
    private func checkIsSQLiteVersionSupported() throws {
        guard sqliteVersion.isSupported else {
            throw SQLiteError.onUnsupportedSQLiteVersion(
                sqliteVersion.major,
                sqliteVersion.minor,
                sqliteVersion.patch
            )
        }
    }
}

private extension SQLiteDatabase {
    class func open(
        at path: String,
        busyTimeout: TimeInterval = 1
    ) throws -> Database {
        guard path != ":memory:" else {
            let config = Configuration()
            let queue = try DatabaseQueue(
                path: path,
                configuration: config
            )
            return .queue(queue)
        }
        
        var config = Configuration()
        config.busyMode = .timeout(busyTimeout)
        config.observesSuspensionNotifications = true
        config.foreignKeysEnabled = true
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
                    throw SQLiteError.onOpen(code, "Could not persist WAL")
                }
            }
        }
        
        let pool = try DatabasePool(path: path, configuration: config)
        return .pool(pool)
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
        try reader.read { try $0.read(sql, arguments: arguments) }
    }
    
    func write(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws {
        try writer.write { try $0.write(sql, arguments: arguments) }
    }
    
    func observe(
        _ sql: SQL,
        arguments: SQLiteArguments,
        queue: OperationQueue
    ) -> AnyPublisher<[SQLiteRow], SQLiteError> {
        let request = SQLRequest(
            sql: sql,
            arguments: arguments.statementArguments
        )
        
        return DatabaseRegionObservation(tracking: [request])
            .publisher(in: self.writer)
            .receive(on: queue)
            .tryMap { _ in
                try self.read(sql, arguments: arguments)
            }
            .mapToSQLiteError()
            .eraseToAnyPublisher()
    }
}

private extension GRDB.Database {
    func read(
        _ sql: SQL,
        arguments: SQLiteArguments
    ) throws -> [SQLiteRow] {
        let statement = try cachedStatement(sql: sql)
        return try Row.fetchAll(
            statement,
            arguments: arguments.isEmpty
                ? nil
                : arguments.statementArguments
        ).compactMap(SQLiteRow.init(row:))
    }
    
    func write(
        _ sql: SQL,
        arguments: SQLiteArguments
    ) throws {
        let statement = try cachedStatement(sql: sql)
        try statement.execute(
            arguments: arguments.isEmpty
                ? nil
                : arguments.statementArguments
        )
    }
    
    @discardableResult
    func execute(raw sql: SQL) throws -> [SQLiteRow] {
        try Row.fetchAll(self, sql: sql)
            .compactMap(SQLiteRow.init(row:))
    }
}

private extension Publisher where Failure: Error {
    func mapToSQLiteError() -> Publishers.MapError<Self, SQLiteError> {
        mapError { error in
            switch error {
            case let err as SQLiteError:
                return err
                
            case let err as DatabaseError:
                return .databaseError(err.resultCode.rawValue)
                
            default:
                return .onInternalError(error as NSError)
            }
        }
    }
}
