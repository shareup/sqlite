import Foundation
import Combine
import SQLite3

extension SQLite {
    public final class Database {
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
            let queue = DispatchQueue(label: "SQLite.Database._queue")
            queue.setSpecific(key: self._queueKey, value: self._queueContext)
            return queue
        }()
        private let _queueKey = DispatchSpecificKey<Int>()
        private lazy var _queueContext: Int = unsafeBitCast(self, to: Int.self)

        private var _cachedStatements = Dictionary<String, Statement>()
        private lazy var _monitor: SQLite.Monitor = { return SQLite.Monitor(database: self) }()
        private let _hook = SQLite.Hook()

        public init(path: String) throws {
            _connection = try SQLite.Database.open(at: path)
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
                _isOpen = false
                SQLite.Database.close(_connection)
            }
        }

        public func inTransaction(_ block: () throws -> Void) throws -> Bool {
            return try sync {
                _transactionCount += 1
                defer { _transactionCount -= 1 }

                do {
                    try execute(raw: "SAVEPOINT database_transaction;")
                    try block()
                    try execute(raw: "RELEASE SAVEPOINT database_transaction;")
                    return true
                } catch {
                    try execute(raw: "ROLLBACK;")
                    return false;
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
                    throw SQLite.Error.onWrite(result)
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

        public func read<T: SQLiteTransformable>(_ sql: SQL, arguments: SQLiteArguments) throws -> Array<T> {
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
}

extension SQLite.Database {
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

extension SQLite.Database {
    public func publisher(_ sql: SQL, arguments: SQLiteArguments = [:],
                          queue: DispatchQueue = .main) -> AnyPublisher<Array<SQLiteRow>, Swift.Error> {
        return SQLite.Publisher(database: self, sql: sql, arguments: arguments, queue: queue)
            .eraseToAnyPublisher()
    }

    public func publisher<T: SQLiteTransformable>(
        _ sql: SQL, arguments: SQLiteArguments = [:], queue: DispatchQueue = .main)
        -> AnyPublisher<Array<T>, Swift.Error> {
            return SQLite.Publisher(database: self, sql: sql, arguments: arguments, queue: queue)
                .compactMap { $0.compactMap { try? T.init(row: $0) } }
                .eraseToAnyPublisher()
    }
}

extension SQLite.Database {
    public func observe(_ sql: SQL, arguments: SQLiteArguments = [:], queue: DispatchQueue = .main,
                        block: @escaping (Array<SQLiteRow>) -> Void) throws -> AnyObject {
        return try sync {
            let (observer, output) = try _observe(sql, arguments: arguments, queue: queue, block: block)
            queue.async { block(output) }
            return observer
        }
    }

    public func observe<T: SQLiteTransformable>(
        _ sql: SQL, arguments: SQLiteArguments = [:], queue: DispatchQueue = .main,
        block: @escaping (Array<T>) -> Void) throws -> AnyObject
    {
        return try sync {
            let updateBlock: (Array<SQLiteRow>) -> Void = { (rows: Array<SQLiteRow>) -> Void in
                let transformed = rows.compactMap { try? T.init(row: $0) }
                block(transformed)
            }
            let (observer, output) = try _observe(sql, arguments: arguments, queue: queue, block: updateBlock)

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

    public func remove(observer: AnyObject) throws {
        sync { _monitor.remove(observer: observer) }
    }

    private var canObserveDatabase: Bool {
        return sync { sqlite3_compileoption_used("SQLITE_ENABLE_COLUMN_METADATA") == Int32(1) }
    }
}

extension SQLite.Database: Equatable {
    public static func == (lhs: SQLite.Database, rhs: SQLite.Database) -> Bool {
        return lhs._connection == rhs._connection
    }
}

extension SQLite.Database {
    public var supportsJSON: Bool {
        return isCompileOptionEnabled("ENABLE_JSON1")
    }

    public func isCompileOptionEnabled(_ name: String) -> Bool {
        return sqlite3_compileoption_used(name) == 1
    }
}

extension SQLite.Database {
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

    func notify(observers: Array<SQLite.Observer>) {
        _queue.async { [weak self] in
            guard let self = self else { return }
            observers.forEach { (observer) in
                defer { observer.statement.reset() }
                guard let (_, output) = try? observer.statement.evaluate() else { return }
                observer.queue.async { observer.block(output) }
            }
        }
    }
}

extension SQLite.Database {
    private func _observe(_ sql: SQL, arguments: SQLiteArguments = [:], queue: DispatchQueue = .main,
                          block: @escaping (Array<SQLiteRow>) -> Void) throws -> (AnyObject, Array<SQLiteRow>) {
        assert(isOnDatabaseQueue)
        guard self.canObserveDatabase else { throw SQLite.Error.onObserveWithoutColumnMetadata }
        let statement = try prepare(sql)
        try statement.bind(arguments: arguments)
        let observer = try _monitor.observe(statement: statement, queue: queue, block: block)

        defer { statement.reset() }

        do {
            let (result, output) = try statement.evaluate()
            if result != SQLITE_DONE && result != SQLITE_INTERRUPT {
                throw SQLite.Error.onStep(result, sql)
            }
            return (observer, output)
        } catch {
            _monitor.remove(observer: observer)
            throw error
        }
    }
}

extension SQLite.Database {
    private func _execute(_ sql: SQL, statement: OpaquePointer,
                          arguments: SQLiteArguments) throws -> Array<SQLiteRow> {
        assert(isOnDatabaseQueue)
        guard _isOpen else { assertionFailure("Database is closed"); return [] }

        try statement.bind(arguments: arguments)
        let (result, output) = try statement.evaluate()

        if result != SQLITE_DONE && result != SQLITE_INTERRUPT {
            throw SQLite.Error.onStep(result, sql)
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

    private func prepare(_ sql: SQL) throws -> Statement {
        var optionalStatement: Statement?
        let result = sqlite3_prepare_v2(_connection, sql, -1, &optionalStatement, nil)
        guard SQLITE_OK == result, let statement = optionalStatement else {
            sqlite3_finalize(optionalStatement)
            throw SQLite.Error.onPrepareStatement(result, sql)
        }
        return statement
    }
}

extension SQLite.Database {
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

extension SQLite.Database {
    private class func open(at path: String) throws -> OpaquePointer {
        var optionalConnection: OpaquePointer?
        let result = sqlite3_open(path, &optionalConnection)

        guard SQLITE_OK == result else {
            SQLite.Database.close(optionalConnection)
            let error = SQLite.Error.onOpen(result, path)
            assertionFailure(error.description)
            throw error
        }

        guard let connection = optionalConnection else {
            let error = SQLite.Error.onOpen(SQLITE_INTERNAL, path)
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
            let error = SQLite.Error.onClose(result)
            assertionFailure(error.description)
        }
    }
}

private extension Statement {
    func bind(arguments: SQLiteArguments) throws {
        for (key, value) in arguments {
            let name = ":\(key)"
            let index = sqlite3_bind_parameter_index(self, name)
            guard index != 0 else { throw SQLite.Error.onGetParameterIndex(key) }
            try bind(value: value, to: index)
        }
    }

    private func bind(value: SQLite.Value, to index: Int32) throws {
        let result: Int32
        switch value {
        case .data(let data):
            result = data.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) -> Int32 in
                return sqlite3_bind_blob(self, index, bytes.baseAddress, Int32(bytes.count), SQLITE_TRANSIENT)
            }
        case .double(let double):
            result = sqlite3_bind_double(self, index, double)
        case .integer(let int):
            result = sqlite3_bind_int64(self, index, int)
        case .null:
            result = sqlite3_bind_null(self, index)
        case .text(let text):
            result = sqlite3_bind_text(self, index, text, -1, SQLITE_TRANSIENT)
        }

        if SQLITE_OK != result {
            throw SQLite.Error.onBindParameter(result, index, value)
        }
    }

    func evaluate() throws -> (Int32, Array<SQLiteRow>) {
        var output = Array<SQLiteRow>()
        var result = sqlite3_step(self)
        while result == SQLITE_ROW {
            try output.append(row())
            result = sqlite3_step(self)
        }
        return (result, output)
    }

    private func row() throws -> SQLiteRow {
        let columnCount = sqlite3_column_count(self)
        guard columnCount > 0 else { return [:] }

        var output = SQLiteRow()
        for column in (0..<columnCount) {
            let name = String(cString: sqlite3_column_name(self, column))
            let value = try self.value(at: column)
            output[name] = value
        }
        return output
    }

    private func value(at column: Int32) throws -> SQLite.Value {
        let type = sqlite3_column_type(self, column)

        switch type {
        case SQLITE_BLOB:
            guard let bytes = sqlite3_column_blob(self, column) else { return .null }
            let count = sqlite3_column_bytes(self, column)
            if count > 0 {
                return .data(Data(bytes: bytes, count: Int(count)))
            } else {
                return .null // Does it make sense to return null if the data is zero bytes?
            }
        case SQLITE_FLOAT:
            return .double(sqlite3_column_double(self, column))
        case SQLITE_INTEGER:
            return .integer(sqlite3_column_int64(self, column))
        case SQLITE_NULL:
            return .null
        case SQLITE_TEXT:
            guard let cString = sqlite3_column_text(self, column) else { return .null }
            return .text(String(cString: cString))
        default:
            throw SQLite.Error.onGetColumnType(type)
        }
    }

    func reset() {
        sqlite3_reset(self)
    }

    func resetAndClearBindings() {
        sqlite3_reset(self)
        sqlite3_clear_bindings(self)
    }
}
