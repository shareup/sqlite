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
