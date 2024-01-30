import Foundation
import GRDB
import SQLite3

public typealias SQLiteArguments = [String: SQLiteValue]
public typealias SQLiteRow = [String: SQLiteValue]

public enum SQLiteValue: Hashable {
    case data(Data)
    case double(Double)
    case integer(Int64)
    case null
    case text(String)
}

extension [String: SQLiteValue] {
    init?(row: Row?) {
        guard let row else { return nil }
        var sqliteRow = SQLiteRow()
        for (name, value) in row {
            switch DatabaseValue(value: value)?.storage {
            case .none, .null:
                sqliteRow[name] = .null

            case let .blob(v):
                sqliteRow[name] = .data(v)

            case let .double(v):
                sqliteRow[name] = .double(v)

            case let .int64(v):
                sqliteRow[name] = .integer(v)

            case let .string(v):
                sqliteRow[name] = .text(v)
            }
        }
        self = sqliteRow
    }

    var statementArguments: StatementArguments {
        var output = [String: DatabaseValueConvertible]()
        for (key, value) in self {
            switch value {
            case let .data(data):
                output[key] = data
            case let .double(double):
                output[key] = double
            case let .integer(int64):
                output[key] = int64
            case .null:
                output[key] = NSNull()
            case let .text(string):
                output[key] = string
            }
        }
        return StatementArguments(output)
    }
}

public extension SQLiteValue {
    var boolValue: Bool? {
        guard case let .integer(int) = self else { return nil }
        return int == 0 ? false : true
    }

    var dataValue: Data? {
        guard case let .data(data) = self else { return nil }
        return data
    }

    var doubleValue: Double? {
        guard case let .double(double) = self else { return nil }
        return double
    }

    var intValue: Int? {
        guard case let .integer(int) = self else { return nil }
        return Int(int)
    }

    var int64Value: Int64? {
        guard case let .integer(int) = self else { return nil }
        return int
    }

    var stringValue: String? {
        guard case let .text(string) = self else { return nil }
        return string
    }
}

public extension [UInt8] {
    var sqliteValue: SQLiteValue {
        .data(Data(self))
    }
}

public extension BinaryInteger {
    var sqliteValue: SQLiteValue {
        .integer(Int64(self))
    }
}

public extension Bool {
    var sqliteValue: SQLiteValue {
        .integer(self ? 1 : 0)
    }
}

public extension Data {
    var sqliteValue: SQLiteValue {
        .data(self)
    }
}

public extension Date {
    var sqliteValue: SQLiteValue {
        .text(PreciseDateFormatter.string(from: self))
    }
}

public extension StringProtocol {
    var sqliteValue: SQLiteValue {
        .text(String(self))
    }
}

public extension [UInt8]? {
    var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            .null
        case let .some(bytes):
            .data(Data(bytes))
        }
    }
}

public extension Optional where Wrapped: BinaryInteger {
    var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            .null
        case let .some(int):
            int.sqliteValue
        }
    }
}

public extension Bool? {
    var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            .null
        case let .some(bool):
            bool.sqliteValue
        }
    }
}

public extension Data? {
    var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            .null
        case let .some(data):
            .data(data)
        }
    }
}

public extension Date? {
    var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            .null
        case let .some(date):
            date.sqliteValue
        }
    }
}

public extension Optional where Wrapped: StringProtocol {
    var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            .null
        case let .some(string):
            string.sqliteValue
        }
    }
}
