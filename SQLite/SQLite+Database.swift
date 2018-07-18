import Foundation
import SQLite3

extension SQLite {
    public final class Database {
        public var userVersion: Int {
            get {
                do {
                    guard let result = try execute(raw: "PRAGMA user_version;").first else { return 0 }
                    return result["user_version"]?.intValue ?? 0
                } catch let error {
                    let message = "Could not get user_version: \(error)"
                    print(message)
                    assertionFailure(message)
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
        fileprivate var _transactionCount = 0

        fileprivate let _connection: OpaquePointer
        fileprivate let _path: String
        fileprivate var _isOpen: Bool

        fileprivate var _cachedStatements = Dictionary<String, OpaquePointer>()

        public init(path: String) throws {
            _connection = try SQLite.Database.open(at: path)
            _isOpen = true
            _path = path
        }

        deinit {
            self.close()
        }

        public func close() {
            guard _isOpen else { return }
            _isOpen = false
            _cachedStatements.values.forEach { sqlite3_finalize($0) }
            SQLite.Database.close(_connection)
        }

        public func inTransaction(_ block: () throws -> Void) throws -> Bool {
            _transactionCount += 1
            defer { _transactionCount -= 1 }
            
            do {
                try execute(raw: "SAVEPOINT database_transaction;")
                try block()
                try execute(raw: "RELEASE SAVEPOINT database_transaction;")
                return true
            } catch let error {
                print("Attempting rollback because transaction failed: \(error)")
                try execute(raw: "ROLLBACK;")
                return false;
            }
        }

        public func write(_ sql: SQL, arguments: SQLiteArguments) throws {
            guard _isOpen else { assertionFailure("Database is closed"); return }

            let statement = try self.statement(for: sql)

            defer {
                sqlite3_reset(statement)
                sqlite3_clear_bindings(statement)
            }

            try bind(arguments: arguments, to: statement)

            let result = sqlite3_step(statement)
            if result != SQLITE_DONE && result != SQLITE_ROW && result != SQLITE_INTERRUPT {
                throw SQLite.Error.onStep(result, sql)
            }
        }

        public func read(_ sql: SQL, arguments: SQLiteArguments) throws -> Array<SQLiteRow> {
            guard _isOpen else { assertionFailure("Database is closed"); return [] }
            let statement = try self.statement(for: sql)
            return try _execute(sql, statement: statement, arguments: arguments)
        }

        @discardableResult
        public func execute(raw sql: SQL) throws -> Array<SQLiteRow> {
            guard _isOpen else { assertionFailure("Database is closed"); return [] }

            let statement = try prepare(sql)
            defer { sqlite3_finalize(statement) }

            return try _execute(sql, statement: statement, arguments: [:])
        }
    }
}

extension SQLite.Database: Equatable {
    public static func == (lhs: SQLite.Database, rhs: SQLite.Database) -> Bool {
        return lhs._connection == rhs._connection
    }
}

extension SQLite.Database {
    fileprivate func _execute(_ sql: SQL, statement: OpaquePointer,
                              arguments: SQLiteArguments) throws -> Array<SQLiteRow> {
        guard _isOpen else { assertionFailure("Database is closed"); return [] }

        defer {
            sqlite3_reset(statement)
            sqlite3_clear_bindings(statement)
        }

        try bind(arguments: arguments, to: statement)

        var output = Array<SQLiteRow>()

        var result = sqlite3_step(statement)
        while result == SQLITE_ROW {
            try output.append(row(for: statement))
            result = sqlite3_step(statement)
        }

        if result != SQLITE_DONE && result != SQLITE_INTERRUPT {
            throw SQLite.Error.onStep(result, sql)
        }

        return output
    }

    fileprivate func statement(for sql: SQL) throws -> OpaquePointer {
        if let cached = _cachedStatements[sql] {
            return cached
        } else {
            let prepared = try prepare(sql)
            _cachedStatements[sql] = prepared
            return prepared
        }
    }

    fileprivate func prepare(_ sql: SQL) throws -> OpaquePointer {
        var optionalStatement: OpaquePointer?
        let result = sqlite3_prepare_v2(_connection, sql, -1, &optionalStatement, nil)
        guard SQLITE_OK == result, let statement = optionalStatement else {
            sqlite3_finalize(optionalStatement)
            let error = SQLite.Error.onPrepareStatement(result, sql)
            assertionFailure(error.description)
            throw error
        }
        return statement
    }

    fileprivate func bind(arguments: SQLiteArguments, to statement: OpaquePointer) throws {
        for (key, value) in arguments {
            let name = ":\(key)"
            let index = sqlite3_bind_parameter_index(statement, name)
            guard index != 0 else { throw SQLite.Error.onGetParameterIndex(key) }
            try bind(value: value, to: index, in: statement)
        }
    }

    fileprivate func bind(value: SQLite.Value, to index: Int32, in statement: OpaquePointer) throws {
        let result: Int32
        switch value {
        case .data(let data):
            result = data.withUnsafeBytes { (bytes: UnsafePointer) -> Int32 in
                return sqlite3_bind_blob(statement, index, bytes, Int32(data.count), SQLITE_TRANSIENT)
            }
        case .double(let double):
            result = sqlite3_bind_double(statement, index, double)
        case .integer(let int):
            result = sqlite3_bind_int64(statement, index, int)
        case .null:
            result = sqlite3_bind_null(statement, index)
        case .text(let text):
            result = sqlite3_bind_text(statement, index, text, -1, SQLITE_TRANSIENT)
        }

        if SQLITE_OK != result {
            throw SQLite.Error.onBindParameter(result, index, value)
        }
    }

    fileprivate func row(for statement: OpaquePointer) throws -> SQLiteRow {
        let columnCount = sqlite3_column_count(statement)
        guard columnCount > 0 else { return [:] }

        var output = SQLiteRow()
        for column in (0..<columnCount) {
            let name = String(cString: sqlite3_column_name(statement, column))
            let value = try self.value(for: statement, at: column)
            output[name] = value
        }
        return output
    }

    private func value(for statement: OpaquePointer, at column: Int32) throws -> SQLite.Value {
        let type = sqlite3_column_type(statement, column)

        switch type {
        case SQLITE_BLOB:
            guard let bytes = sqlite3_column_blob(statement, column) else { return .null }
            let count = sqlite3_column_bytes(statement, column)
            if count > 0 {
                return .data(Data(bytes: bytes, count: Int(count)))
            } else {
                return .null // Does it make sense to return null if the data is zero bytes?
            }
        case SQLITE_FLOAT:
            return .double(sqlite3_column_double(statement, column))
        case SQLITE_INTEGER:
            return .integer(sqlite3_column_int64(statement, column))
        case SQLITE_NULL:
            return .null
        case SQLITE_TEXT:
            guard let cString = sqlite3_column_text(statement, column) else { return .null }
            return .text(String(cString: cString))
        default:
            throw SQLite.Error.onGetColumnType(type)
        }
    }
}

extension SQLite.Database {
    fileprivate class func open(at path: String) throws -> OpaquePointer {
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

    fileprivate class func close(_ connection: OpaquePointer?) {
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
