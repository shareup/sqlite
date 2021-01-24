import Foundation
import SQLite3

extension SQLiteStatement {
    func bind(arguments: SQLiteArguments) throws {
        for (key, value) in arguments {
            let name = ":\(key)"
            let index = sqlite3_bind_parameter_index(self, name)
            guard index != 0 else { throw SQLiteError.onGetParameterIndex(key) }
            try bind(value: value, to: index)
        }
    }

    private func bind(value: SQLiteValue, to index: Int32) throws {
        let result: Int32
        switch value {
        case .data(let data):
            result = data.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) -> Int32 in
                return sqlite3_bind_blob(
                    self,
                    index,
                    bytes.baseAddress,
                    Int32(bytes.count),
                    SQLITE_TRANSIENT
                )
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
            throw SQLiteError.onBindParameter(result, index, value)
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

    private func value(at column: Int32) throws -> SQLiteValue {
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
            throw SQLiteError.onGetColumnType(type)
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
