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

    public var hasOpenTransactions: Bool { return _transactionCount != 0 }
    private var _transactionCount = 0

    private let _connection: OpaquePointer
    private let _path: String
    private var _isOpen: Bool

    private lazy var _queue: DispatchQueue = {
        let queue = DispatchQueue(
            label: "app.shareup.sqlite.sqlitedatabase.queue",
            qos: .default,
            attributes: [],
            autoreleaseFrequency: .workItem,
            target: .global()
        )
        queue.setSpecific(key: self._queueKey, value: self._queueContext)
        return queue
    }()
    private let _queueKey = DispatchSpecificKey<Int>()
    private lazy var _queueContext: Int = unsafeBitCast(self, to: Int.self)

    private var _cachedStatements = Dictionary<String, SQLiteStatement>()
    private lazy var _monitor: Monitor = { return Monitor(database: self) }()
    private let _hook = Hook()

    public init(path: String = ":memory:") throws {
        _connection = try SQLiteDatabase.open(at: path)
        _isOpen = true
        _path = path
    }

    deinit {
        self.close()
    }

    public func close() {
        sync {
            guard _isOpen else { return }
            _monitor.removeAllObservers()
            _cachedStatements.values.forEach { sqlite3_finalize($0) }
            _cachedStatements.removeAll()
            _isOpen = false
            SQLiteDatabase.close(_connection)
        }
    }
}

// MARK: -
// MARK: Asynchronous queries

extension SQLiteDatabase {
    public func inTransactionPublisher<T>(
        _ block: @escaping (SQLiteDatabase) throws -> T
    ) -> AnyPublisher<T, SQLiteError> {
        SQLiteFuture { [_queue] (promise) in
            _queue.async { [weak self] in
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
                    promise(.failure(.onInternalError(error.localizedDescription)))
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
                return .onInternalError(error.localizedDescription)
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
        arguments: SQLiteArguments
    ) -> AnyPublisher<[T], SQLiteError> {
        readPublisher(sql, arguments: arguments)
            .tryMap { try $0.map { try T.init(row: $0) } }
            .mapError { (error) -> SQLiteError in
                if let sqliteError = error as? SQLiteError {
                    return sqliteError
                } else {
                    return .onInternalError(error.localizedDescription)
                }
            }
            .eraseToAnyPublisher()
    }
}

// MARK: -
// MARK: Synchronous queries

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

    public func write(_ sql: SQL, arguments: SQLiteArguments) throws {
        try sync {
            guard _isOpen else { assertionFailure("Database is closed"); return }

            let statement = try self.cachedStatement(for: sql)
            defer { statement.resetAndClearBindings() }

            let result = try _execute(sql, statement: statement, arguments: arguments)
            if result.isEmpty == false {
                throw SQLiteError.onWrite(result)
            }
        }
    }

    public func read(_ sql: SQL, arguments: SQLiteArguments) throws -> Array<SQLiteRow> {
        return try sync {
            guard _isOpen else { assertionFailure("Database is closed"); return [] }
            let statement = try self.cachedStatement(for: sql)
            defer { statement.resetAndClearBindings() }
            return try _execute(sql, statement: statement, arguments: arguments)
        }
    }

    public func read<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments
    ) throws -> Array<T> {
        return try sync {
            let rows: Array<SQLiteRow> = try read(sql, arguments: arguments)
            return try rows.map { try T.init(row: $0) }
        }
    }

    @discardableResult
    public func execute(raw sql: SQL) throws -> Array<SQLiteRow> {
        return try sync {
            guard _isOpen else { assertionFailure("Database is closed"); return [] }

            let statement = try prepare(sql)
            defer { sqlite3_finalize(statement) }

            return try _execute(sql, statement: statement, arguments: [:])
        }
    }
}

// MARK: -
// MARK: Tables and columns

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

// MARK: -
// MARK: Combine Publishers observing SQL queries

extension SQLiteDatabase {
    public func publisher(
        _ sql: SQL,
        arguments: SQLiteArguments = [:],
        queue: DispatchQueue = .main
    ) -> AnyPublisher<Array<SQLiteRow>, Swift.Error> {
        return SQLitePublisher(database: self, sql: sql, arguments: arguments, queue: queue)
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
        arguments: SQLiteArguments = [:],
        queue: DispatchQueue = .main
    ) -> AnyPublisher<Array<T>, Swift.Error> {
        return publisher(sql, arguments: arguments, queue: queue) as AnyPublisher<Array<T>, Swift.Error>
    }

    public func publisher<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:],
        queue: DispatchQueue = .main
    ) -> AnyPublisher<Array<T>, Swift.Error> {
        return SQLitePublisher(database: self, sql: sql, arguments: arguments, queue: queue)
            .tryMap { try $0.map { try T.init(row: $0) } }
            .eraseToAnyPublisher()
    }
}

// MARK: -
// MARK: Block-based observation of SQL queries

extension SQLiteDatabase {
    func observe(
        _ sql: SQL,
        arguments: SQLiteArguments = [:],
        queue: DispatchQueue = .main,
        block: @escaping (Array<SQLiteRow>) -> Void
    ) throws -> AnyObject {
        return try sync {
            let (observer, output) = try _observe(
                sql,
                arguments: arguments,
                queue: queue,
                block: block
            )
            queue.async { block(output) }
            return observer
        }
    }

    func observe<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments = [:],
        queue: DispatchQueue = .main,
        block: @escaping (Array<T>) -> Void
    ) throws -> AnyObject {
        return try sync {
            let updateBlock: (Array<SQLiteRow>) -> Void = { (rows: Array<SQLiteRow>) -> Void in
                let transformed = rows.compactMap { try? T.init(row: $0) }
                block(transformed)
            }
            let (observer, output) = try _observe(
                sql,
                arguments: arguments,
                queue: queue,
                block: updateBlock
            )

            var transformed: Array<T> = []
            do {
                transformed = try output.map { try T.init(row: $0) }
                queue.async { block(transformed) }
                return observer
            } catch {
                _monitor.remove(observer: observer)
                throw error
            }
        }
    }

    func remove(observer: AnyObject) throws {
        sync { _monitor.remove(observer: observer) }
    }

    private var canObserveDatabase: Bool {
        return sync { sqlite3_compileoption_used("SQLITE_ENABLE_COLUMN_METADATA") == Int32(1) }
    }
}

// MARK: -
// MARK: Equatable

extension SQLiteDatabase: Equatable {
    public static func == (lhs: SQLiteDatabase, rhs: SQLiteDatabase) -> Bool {
        return lhs._connection == rhs._connection
    }
}

// MARK: -
// MARK: Compile-time SQLite options

extension SQLiteDatabase {
    public var supportsJSON: Bool {
        return isCompileOptionEnabled("ENABLE_JSON1")
    }

    public func isCompileOptionEnabled(_ name: String) -> Bool {
        return sqlite3_compileoption_used(name) == 1
    }
}

// MARK: -
// MARK: Vacuuming

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

// MARK: -
// MARK: SQLite hooks

extension SQLiteDatabase {
    func createUpdateHandler(_ block: @escaping (String) -> Void) {
        let updateBlock: UpdateHookCallback = { _, _, _, tableName, _ in
            guard let tableName = tableName else { return }
            block(String(cString: tableName))
        }

        _hook.update = updateBlock
        let hookAsContext = Unmanaged.passUnretained(_hook).toOpaque()
        sqlite3_update_hook(_connection, updateHookWrapper, hookAsContext)
    }

    func removeUpdateHandler() {
        sqlite3_update_hook(_connection, nil, nil)
    }

    func createCommitHandler(_ block: @escaping () -> Void) {
        _hook.commit = block
        let hookAsContext = Unmanaged.passUnretained(_hook).toOpaque()
        sqlite3_commit_hook(_connection, commitHookWrapper, hookAsContext)
    }

    func removeCommitHandler() {
        sqlite3_commit_hook(_connection, nil, nil)
    }

    func createRollbackHandler(_ block: @escaping () -> Void) {
        _hook.rollback = block
        let hookAsContext = Unmanaged.passUnretained(_hook).toOpaque()
        sqlite3_rollback_hook(_connection, rollbackHookWrapper, hookAsContext)
    }

    func removeRollbackHandler() {
        sqlite3_rollback_hook(_connection, nil, nil)
    }

    func notify(observers: Array<Observer>) {
        _queue.async {
            observers.forEach { (observer) in
                defer { observer.statement.reset() }
                guard let (_, output) = try? observer.statement.evaluate() else { return }
                observer.queue.async { observer.block(output) }
            }
        }
    }
}

// MARK: -
// MARK: Private

extension SQLiteDatabase {
    private func _observe(
        _ sql: SQL,
        arguments: SQLiteArguments = [:],
        queue: DispatchQueue = .main,
        block: @escaping (Array<SQLiteRow>) -> Void
    ) throws -> (AnyObject, Array<SQLiteRow>) {
        assert(isOnDatabaseQueue)
        guard self.canObserveDatabase else { throw SQLiteError.onObserveWithoutColumnMetadata }
        let statement = try prepare(sql)
        try statement.bind(arguments: arguments)
        let observer = try _monitor.observe(statement: statement, queue: queue, block: block)

        defer { statement.reset() }

        do {
            let (result, output) = try statement.evaluate()
            if result != SQLITE_DONE && result != SQLITE_INTERRUPT {
                throw SQLiteError.onStep(result, sql)
            }
            return (observer, output)
        } catch {
            _monitor.remove(observer: observer)
            throw error
        }
    }
}

extension SQLiteDatabase {
    private func _executeAsync(
        _ sql: SQL,
        arguments: SQLiteArguments,
        prepareStatement: @escaping () throws -> OpaquePointer,
        resetStatement: @escaping (OpaquePointer) -> Void
    ) -> SQLiteFuture<[SQLiteRow]> {
        SQLiteFuture { [_queue] (promise) in
            _queue.async { [weak self] in
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
                    promise(.failure(.onInternalError(error.localizedDescription)))
                }
            }
        }
    }

    private func _execute(
        _ sql: SQL,
        statement: OpaquePointer,
        arguments: SQLiteArguments
    ) throws -> [SQLiteRow] {
        assert(isOnDatabaseQueue)
        guard _isOpen else { throw SQLiteError.onExecuteQueryWithoutOpenDatabase }

        try statement.bind(arguments: arguments)
        let (result, output) = try statement.evaluate()

        if result != SQLITE_DONE && result != SQLITE_INTERRUPT {
            throw SQLiteError.onStep(result, sql)
        }

        return output
    }

    private func cachedStatement(for sql: SQL) throws -> OpaquePointer {
        if let cached = _cachedStatements[sql] {
            return cached
        } else {
            let prepared = try prepare(sql)
            _cachedStatements[sql] = prepared
            return prepared
        }
    }

    private func prepare(_ sql: SQL) throws -> SQLiteStatement {
        var optionalStatement: SQLiteStatement?
        let result = sqlite3_prepare_v2(_connection, sql, -1, &optionalStatement, nil)
        guard SQLITE_OK == result, let statement = optionalStatement else {
            sqlite3_finalize(optionalStatement)
            throw SQLiteError.onPrepareStatement(result, sql)
        }
        return statement
    }
}

extension SQLiteDatabase {
    private var isOnDatabaseQueue: Bool {
        return DispatchQueue.getSpecific(key: _queueKey) == _queueContext
    }

    private func sync<T>(_ block: () throws -> T) rethrows -> T {
        if isOnDatabaseQueue {
            return try block()
        } else {
            return try _queue.sync(execute: block)
        }
    }
}

extension SQLiteDatabase {
    private class func open(at path: String) throws -> OpaquePointer {
        var optionalConnection: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let result = sqlite3_open_v2(path, &optionalConnection, flags, nil)

        guard SQLITE_OK == result else {
            SQLiteDatabase.close(optionalConnection)
            let error = SQLiteError.onOpen(result, path)
            assertionFailure(error.description)
            throw error
        }

        guard let connection = optionalConnection else {
            let error = SQLiteError.onOpen(SQLITE_INTERNAL, path)
            assertionFailure(error.description)
            throw error
        }

        return connection
    }

    private class func close(_ connection: OpaquePointer?) {
        guard let connection = connection else { return }
        let result = sqlite3_close_v2(connection)
        if result != SQLITE_OK {
            // We don't actually throw here, because the `sqlite3_close_v2()` will
            // clean up the SQLite database connection when the transactions that
            // were preventing the close are finalized.
            // https://sqlite.org/c3ref/close.html
            let error = SQLiteError.onClose(result)
            assertionFailure(error.description)
        }
    }
}
