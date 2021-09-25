import Foundation
import Combine
import SQLite3

public final class SQLiteDatabase {
    public var userVersion: Int {
        get {
            do {
                guard let result = try execute(raw: "PRAGMA user_version;").first else { return 0 }
                return result["user_version"]?.intValue ?? 0
            } catch let error {
                assertionFailure("Could not get user_version: \(error)")
                return 0
            }
        }
        set {
            do {
                try execute(raw: "PRAGMA user_version = \(newValue);")
            } catch let error {
                assertionFailure("Could not set user_version to \(newValue): \(error)")
            }
        }
    }

    public var path: String { _path }
    internal var sqliteConnection: OpaquePointer { sync { _connection } }

    public var hasOpenTransactions: Bool { return _transactionCount != 0 }
    private var _transactionCount = 0

    private var _connection: OpaquePointer
    private let _path: String
    private var _isOpen: Bool

    internal var sqliteVersion: SQLiteVersion { _sqliteVersion }
    private var _sqliteVersion: SQLiteVersion!

    private var _cachedStatements = Dictionary<String, SQLiteStatement>()
    private var _changePublisher: SQLiteDatabaseChangePublisher!
    private let _hook = Hook()

    public static func makeShared(
        path: String,
        busyTimeout seconds: TimeInterval = 0
    ) throws -> SQLiteDatabase {
        guard path != ":memory:", let url = URL(string: path)
        else { throw SQLiteError.onInvalidPath(path) }

        let coordinator = NSFileCoordinator(filePresenter: nil)

        var database: SQLiteDatabase? = nil
        var fileCoordinatorError: NSError? = nil
        var databaseError: Error? = nil

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

        guard let db = database, fileCoordinatorError == nil, databaseError == nil else {
            let error = String(describing: fileCoordinatorError ?? databaseError)
            throw SQLiteError.onOpenSharedDatabase(path, error)
        }

        sqlite3_busy_timeout(db.sqliteConnection, Int32(seconds * 1000))

        return db
    }

    public init(path: String = ":memory:") throws {
        _connection = try SQLiteDatabase.open(at: path)
        _isOpen = true
        _path = path
        _sqliteVersion = try getSQLiteVersion()
        try checkIsSQLiteVersionSupported()
        _changePublisher = SQLiteDatabaseChangePublisher(database: self)
    }

    deinit {
        try? close()
    }

    public func reopen() throws {
        try sync {
            guard !_isOpen else { return }
            _connection = try SQLiteDatabase.open(at: _path)
            _isOpen = true
            _changePublisher.open()
        }
    }

    public func close() throws {
        try sync {
            guard _isOpen else { return }
            _changePublisher.close()
            _cachedStatements.values.forEach { sqlite3_finalize($0) }
            _cachedStatements.removeAll()
            _isOpen = false
            try SQLiteDatabase.close(_connection)
        }
    }
}

// MARK: - Asynchronous queries

extension SQLiteDatabase {
    public func inTransactionPublisher<T>(
        _ block: @escaping (SQLiteDatabase) throws -> T
    ) -> AnyPublisher<T, SQLiteError> {
        SQLiteFuture { (promise) in
            SQLiteQueue.async { [weak self] in
                guard let self = self else {
                    promise(.failure(.onExecuteQueryAfterDeallocating))
                    return
                }

                do {
                    let result = try self.inTransaction(block)
                    promise(.success(result))
                } catch let error as SQLiteError {
                    promise(.failure(error))
                } catch {
                    promise(.failure(.onInternalError(error as NSError)))
                }
            }
        }
        .eraseToAnyPublisher()
    }

    public func writePublisher(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<Void, SQLiteError> {
        let prepareStatement = { [unowned self] () throws -> OpaquePointer in
            try self.cachedStatement(for: sql)
        }

        let resetStatement = { (statement: OpaquePointer) -> Void in
            statement.resetAndClearBindings()
        }

        return _executeAsync(
            sql,
            arguments: arguments,
            prepareStatement: prepareStatement,
            resetStatement: resetStatement
        )
        .tryMap { rows in throw SQLiteError.onWrite(rows) }
        .mapError { (error) -> SQLiteError in
            if let sqliteError = error as? SQLiteError {
                return sqliteError
            } else {
                return .onInternalError(error as NSError)
            }
        }
        .eraseToAnyPublisher()
    }

    public func readPublisher(_ sql: SQL, arguments: SQLiteArguments = [:]) -> AnyPublisher<[SQLiteRow], SQLiteError> {
        let prepareStatement = { [unowned self] () throws -> OpaquePointer in
            try self.cachedStatement(for: sql)
        }

        let resetStatement = { (statement: OpaquePointer) -> Void in
            statement.resetAndClearBindings()
        }

        return _executeAsync(
            sql,
            arguments: arguments,
            prepareStatement: prepareStatement,
            resetStatement: resetStatement
        )
        .eraseToAnyPublisher()
    }

    public func readPublisher<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<[T], SQLiteError> {
        readPublisher(sql, arguments: arguments)
            .tryMap { try $0.map { try T.init(row: $0) } }
            .mapError { (error) -> SQLiteError in
                if let sqliteError = error as? SQLiteError {
                    return sqliteError
                } else {
                    return .onInternalError(error as NSError)
                }
            }
            .eraseToAnyPublisher()
    }
}

// MARK: - Synchronous queries

extension SQLiteDatabase {
    public func inTransaction<T>(_ block: (SQLiteDatabase) throws -> T) rethrows -> T {
        return try sync {
            _transactionCount += 1
            defer { _transactionCount -= 1 }

            do {
                try execute(raw: "SAVEPOINT database_transaction;")
                let result = try block(self)
                try execute(raw: "RELEASE SAVEPOINT database_transaction;")
                return result
            } catch {
                try execute(raw: "ROLLBACK;")
                throw error
            }
        }
    }

    public func write(_ sql: SQL, arguments: SQLiteArguments = [:]) throws {
        try sync {
            guard _isOpen else { throw SQLiteError.databaseIsClosed }

            let statement = try self.cachedStatement(for: sql)
            defer { statement.resetAndClearBindings() }

            let result = try _execute(sql, statement: statement, arguments: arguments)
            if result.isEmpty == false {
                throw SQLiteError.onWrite(result)
            }
        }
    }

    public func read(_ sql: SQL, arguments: SQLiteArguments = [:]) throws -> Array<SQLiteRow> {
        return try sync {
            guard _isOpen else { throw SQLiteError.databaseIsClosed }
            let statement = try self.cachedStatement(for: sql)
            defer { statement.resetAndClearBindings() }
            return try _execute(sql, statement: statement, arguments: arguments)
        }
    }

    public func read<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws -> Array<T> {
        return try sync {
            let rows: Array<SQLiteRow> = try read(sql, arguments: arguments)
            return try rows.map { try T.init(row: $0) }
        }
    }

    @discardableResult
    public func execute(raw sql: SQL) throws -> Array<SQLiteRow> {
        return try sync {
            guard _isOpen else { throw SQLiteError.databaseIsClosed }

            let statement = try prepare(sql)
            defer { sqlite3_finalize(statement) }

            return try _execute(sql, statement: statement, arguments: [:])
        }
    }
}

// MARK: - Tables and columns

extension SQLiteDatabase {
    public func tables() throws -> Array<String> {
        return try sync {
            let sql = "SELECT * FROM sqlite_master WHERE type='table';"
            let statement = try cachedStatement(for: sql)
            let tablesResult = try _execute(sql, statement: statement, arguments: [:])
            statement.resetAndClearBindings()
            return tablesResult.compactMap { $0["tbl_name"]?.stringValue }
        }
    }

    public func columns(in table: String) throws -> Array<String> {
        return try sync {
            let sql = columnsSQL(for: table)
            let statement = try cachedStatement(for: sql)
            let columnsResult = try _execute(sql, statement: statement, arguments: [:])
            statement.resetAndClearBindings()
            return columnsResult.compactMap { $0["name"]?.stringValue }
        }
    }

    private func columnsSQL(for table: String) -> SQL {
        return """
        SELECT DISTINCT ti.name AS name, ti.pk AS pk
            FROM sqlite_master AS m, pragma_table_info(m.name) as ti
            WHERE m.type='table'
            AND m.name='\(table)';
        """
    }
}

// MARK: - Combine Publishers observing SQL queries

extension SQLiteDatabase {
    public func publisher(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<Array<SQLiteRow>, SQLiteError> {
        guard canSubscribeToDatabase else {
            return Fail(
                outputType: Array<SQLiteRow>.self,
                failure: SQLiteError.onSubscribeWithoutColumnMetadata
            ).eraseToAnyPublisher()
        }

        return _changePublisher
            .results(sql: sql, arguments: arguments, database: self)
            .eraseToAnyPublisher()
    }

    // Swift favors type inference and, consequently, does not allow specializing functions at the call site.
    // This means that combining multiple `Combine.Publisher` together can be frustrating because
    // Swift can't infer the type. Adding this function that includes the generic type means we don't
    // need to specify the type at the call site using `as Array<T>`.
    // https://forums.swift.org/t/compiler-cannot-infer-the-type-of-a-generic-method-cannot-specialize-a-non-generic-definition/10294
    public func publisher<T: SQLiteTransformable>(
        _ type: T.Type,
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<Array<T>, SQLiteError> {
        return publisher(sql, arguments: arguments) as AnyPublisher<Array<T>, SQLiteError>
    }

    public func publisher<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:]
    ) -> AnyPublisher<Array<T>, SQLiteError> {
        guard canSubscribeToDatabase else {
            return Fail(
                outputType: Array<T>.self,
                failure: SQLiteError.onSubscribeWithoutColumnMetadata
            ).eraseToAnyPublisher()
        }

        return _changePublisher
            .results(sql: sql, arguments: arguments, database: self)
            .tryMap { try $0.map { try T.init(row: $0) } }
            .mapError { (error: Swift.Error) -> SQLiteError in
                if let sqliteError = error as? SQLiteError {
                    return sqliteError
                } else {
                    return SQLiteError.onDecodingRow(String(describing: error))
                }
            }.eraseToAnyPublisher()
    }

    private var canSubscribeToDatabase: Bool {
        return sync { sqlite3_compileoption_used("SQLITE_ENABLE_COLUMN_METADATA") == Int32(1) }
    }
}

// MARK: - Equatable

extension SQLiteDatabase: Equatable {
    public static func == (lhs: SQLiteDatabase, rhs: SQLiteDatabase) -> Bool {
        return ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
    }
}

// MARK: - Compile-time SQLite options

extension SQLiteDatabase {
    public var supportsJSON: Bool {
        return isCompileOptionEnabled("ENABLE_JSON1")
    }

    public func isCompileOptionEnabled(_ name: String) -> Bool {
        return sqlite3_compileoption_used(name) == 1
    }
}

// MARK: - Vacuuming

extension SQLiteDatabase {
    public enum AutoVacuumMode: Int {
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

    public var autoVacuumMode: AutoVacuumMode {
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

    public func incrementalVacuum(_ pages: Int? = nil) throws {
        let sql: SQL
        if let pages = pages {
            sql = "PRAGMA incremental_vacuum(\(pages));"
        } else {
            sql = "PRAGMA incremental_vacuum;"
        }
        try execute(raw: sql)
    }

    public func vacuum() throws {
        try execute(raw: "VACUUM;")
    }
}

// MARK: - SQLite hooks

extension SQLiteDatabase {
    func createUpdateHandler(_ block: @escaping (String) -> Void) {
        precondition(SQLiteQueue.isCurrentQueue)

        let updateBlock: UpdateHookCallback = { _, _, _, tableName, _ in
            precondition(SQLiteQueue.isCurrentQueue)
            guard let tableName = tableName else { return }
            block(String(cString: tableName))
        }

        _hook.update = updateBlock
        let hookAsContext = Unmanaged.passUnretained(_hook).toOpaque()
        sqlite3_update_hook(_connection, updateHookWrapper, hookAsContext)
    }

    func removeUpdateHandler() {
        precondition(SQLiteQueue.isCurrentQueue)
        sqlite3_update_hook(_connection, nil, nil)
        _hook.update = nil
    }

    func createCommitHandler(_ block: @escaping () -> Void) {
        precondition(SQLiteQueue.isCurrentQueue)
        _hook.commit = block
        let hookAsContext = Unmanaged.passUnretained(_hook).toOpaque()
        sqlite3_commit_hook(_connection, commitHookWrapper, hookAsContext)
    }

    func removeCommitHandler() {
        precondition(SQLiteQueue.isCurrentQueue)
        sqlite3_commit_hook(_connection, nil, nil)
        _hook.commit = nil
    }

    func createRollbackHandler(_ block: @escaping () -> Void) {
        precondition(SQLiteQueue.isCurrentQueue)
        _hook.rollback = block
        let hookAsContext = Unmanaged.passUnretained(_hook).toOpaque()
        sqlite3_rollback_hook(_connection, rollbackHookWrapper, hookAsContext)
    }

    func removeRollbackHandler() {
        precondition(SQLiteQueue.isCurrentQueue)
        sqlite3_rollback_hook(_connection, nil, nil)
        _hook.rollback = nil
    }
}

// MARK: - Private

extension SQLiteDatabase {
    private func _executeAsync(
        _ sql: SQL,
        arguments: SQLiteArguments,
        prepareStatement: @escaping () throws -> OpaquePointer,
        resetStatement: @escaping (OpaquePointer) -> Void
    ) -> SQLiteFuture<[SQLiteRow]> {
        SQLiteFuture { (promise) in
            SQLiteQueue.async { [weak self] in
                guard let self = self else {
                    promise(.failure(.onExecuteQueryAfterDeallocating))
                    return
                }

                do {
                    let statement = try prepareStatement()
                    defer { resetStatement(statement) }

                    let result = try self._execute(
                        sql,
                        statement: statement,
                        arguments: arguments
                    )
                    promise(.success(result))
                } catch let error as SQLiteError {
                    promise(.failure(error))
                } catch {
                    promise(.failure(.onInternalError(error as NSError)))
                }
            }
        }
    }

    private func _execute(
        _ sql: SQL,
        statement: OpaquePointer,
        arguments: SQLiteArguments
    ) throws -> [SQLiteRow] {
        precondition(SQLiteQueue.isCurrentQueue)
        guard _isOpen else { throw SQLiteError.databaseIsClosed }

        try statement.bind(arguments: arguments)
        let (result, output) = try statement.evaluate()

        if result != SQLITE_DONE && result != SQLITE_INTERRUPT {
            throw SQLiteError.onStep(result, sql)
        }

        return output
    }

    private func cachedStatement(for sql: SQL) throws -> OpaquePointer {
        precondition(SQLiteQueue.isCurrentQueue)
        if let cached = _cachedStatements[sql] {
            return cached
        } else {
            let prepared = try prepare(sql)
            _cachedStatements[sql] = prepared
            return prepared
        }
    }

    private func prepare(_ sql: SQL) throws -> SQLiteStatement {
        precondition(SQLiteQueue.isCurrentQueue)
        guard _isOpen else { throw SQLiteError.databaseIsClosed }
        return try SQLiteStatement.prepare(sql, in: self)
    }
}

extension SQLiteDatabase {
    private func sync<T>(_ block: () throws -> T) rethrows -> T {
        try SQLiteQueue.sync(block)
    }
}

extension SQLiteDatabase {
    private func getSQLiteVersion() throws -> SQLiteVersion {
        return try SQLiteVersion(
            rows: try execute(raw: SQLiteVersion.selectVersion)
        )
    }

    private func checkIsSQLiteVersionSupported() throws {
        guard _sqliteVersion.isSupported else {
            throw SQLiteError.onUnsupportedSQLiteVersion(
                _sqliteVersion.major,
                _sqliteVersion.minor,
                _sqliteVersion.patch
            )
        }
    }
}

extension SQLiteDatabase {
    private class func open(at path: String) throws -> OpaquePointer {
        var optionalConnection: OpaquePointer?
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        var result = sqlite3_open_v2(path, &optionalConnection, flags, nil)

        guard SQLITE_OK == result else {
            try SQLiteDatabase.close(optionalConnection)
            let error = SQLiteError.onOpen(result, path)
            throw error
        }

        guard let connection = optionalConnection else {
            let error = SQLiteError.onOpen(SQLITE_INTERNAL, path)
            throw error
        }

        result = sqlite3_exec(connection, "PRAGMA journal_mode=WAL;", nil, nil, nil)
        guard result == SQLITE_OK else { throw SQLiteError.onEnableWAL(result) }

        return connection
    }

    private class func close(_ connection: OpaquePointer?) throws {
        guard let connection = connection else { return }
        let result = sqlite3_close(connection)
        guard result == SQLITE_OK else { throw SQLiteError.onClose(result) }
    }
}
