import Foundation
import SQLite3

public typealias SQLiteArguments = Dictionary<String, SQLite.Value>
public typealias SQLiteRow = Dictionary<String, SQLite.Value>

extension SQLite {
    public enum Value {
        case data(Data)
        case double(Double)
        case integer(Int64)
        case null
        case text(String)
    }
}

extension SQLite.Value {
    public init(rawValue: OpaquePointer) {
        switch sqlite3_value_type(rawValue) {
        case SQLITE_BLOB:
            if let bytes = sqlite3_value_blob(rawValue) {
                self = .data(Data(bytes: bytes, count: Int(sqlite3_value_bytes(rawValue))))
            } else {
                self = .data(Data())
            }
        case SQLITE_FLOAT:
            self = .double(sqlite3_value_double(rawValue))
        case SQLITE_INTEGER:
            self = .integer(sqlite3_value_int64(rawValue))
        case SQLITE_NULL:
            self = .null
        case SQLITE_TEXT:
            self = .text(String(cString: sqlite3_value_text(rawValue)))
        default:
            fatalError("\(rawValue) is not a valid `sqlite3_value`")
        }
    }
}

extension SQLite.Value {
    public var boolValue: Bool? {
        guard case .integer(let int) = self else { return nil }
        return int == 0 ? false : true
    }

    public var dataValue: Data? {
        guard case .data(let data) = self else { return nil }
        return data
    }

    public var doubleValue: Double? {
        guard case .double(let double) = self else { return nil }
        return double
    }

    public var intValue: Int? {
        guard case .integer(let int) = self else { return nil }
        return Int(int)
    }

    public var int64Value: Int64? {
        guard case .integer(let int) = self else { return nil }
        return int
    }

    public var stringValue: String? {
        guard case .text(let string) = self else { return nil }
        return string
    }
}

extension SQLite.Value: Hashable {
    public static func == (lhs: SQLite.Value, rhs: SQLite.Value) -> Bool {
        switch (lhs, rhs) {
        case (.data(let left), .data(let right)):
            return left == right
        case (.double(let left), .double(let right)):
            return left == right
        case (.integer(let left), .integer(let right)):
            return left == right
        case (.null, .null):
            return true
        case (.text(let left), .text(let right)):
            return left == right
        default:
            return false
        }
    }

    public var hashValue: Int {
        switch self {
        case .data(let value):
            return value.hashValue
        case .double(let value):
            return value.hashValue
        case .integer(let value):
            return value.hashValue
        case .null:
            return 0
        case .text(let value):
            return value.hashValue
        }
    }
}

extension Int32 {
    var columnType: String {
        switch self {
        case SQLITE_INTEGER:
            return "INTEGER"
        case SQLITE_FLOAT:
            return "DOUBLE"
        case SQLITE_BLOB:
            return "BLOB"
        case SQLITE_TEXT:
            return "TEXT"
        case SQLITE_NULL:
            return "NULL"
        default:
            fatalError("\(self) is not a valid SQLite column type")
        }
    }
}
