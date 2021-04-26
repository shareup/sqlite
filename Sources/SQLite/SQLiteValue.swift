import Foundation
import SQLite3

public typealias SQLiteArguments = Dictionary<String, SQLiteValue>
public typealias SQLiteRow = Dictionary<String, SQLiteValue>

public enum SQLiteValue: Hashable {
    case data(Data)
    case double(Double)
    case integer(Int64)
    case null
    case text(String)
}

extension SQLiteValue {
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

extension Array where Element == UInt8 {
    public var sqliteValue: SQLiteValue {
        .data(Data(self))
    }
}

extension BinaryInteger {
    public var sqliteValue: SQLiteValue {
        .integer(Int64(self))
    }
}

extension Bool {
    public var sqliteValue: SQLiteValue {
        .integer(self ? 1 : 0)
    }
}

extension Data {
    public var sqliteValue: SQLiteValue {
        .data(self)
    }
}

extension Date {
    public var sqliteValue: SQLiteValue {
        .text(PreciseDateFormatter.string(from: self))
    }
}

extension StringProtocol {
    public var sqliteValue: SQLiteValue {
        .text(String(self))
    }
}

extension Optional where Wrapped == Array<UInt8> {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(bytes):
            return .data(Data(bytes))
        }
    }
}

extension Optional where Wrapped: BinaryInteger {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(int):
            return int.sqliteValue
        }
    }
}

extension Optional where Wrapped == Bool {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(bool):
            return bool.sqliteValue
        }
    }
}

extension Optional where Wrapped == Data {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(data):
            return .data(data)
        }
    }
}

extension Optional where Wrapped == Date {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(date):
            return date.sqliteValue
        }
    }
}

extension Optional where Wrapped: StringProtocol {
    public var sqliteValue: SQLiteValue {
        switch self {
        case .none:
            return .null
        case let .some(string):
            return string.sqliteValue
        }
    }
}
