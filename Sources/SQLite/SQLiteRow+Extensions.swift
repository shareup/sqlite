import Foundation

public extension Dictionary where Dictionary.Key == String, Dictionary.Value == SQLiteValue {
    func optionalValue<V>(for key: CodingKey) -> V? {
        try? value(for: key)
    }

    func optionalValue<V>(for key: String) -> V? {
        try? value(for: key)
    }

    func value<V>(for key: CodingKey) throws -> V {
        try value(for: key.stringValue)
    }

    func value<V>(for key: String) throws -> V {
        if String.self == V.self {
            guard let value = self[key]?.stringValue
            else { throw SQLiteError.onDecodingRow(key) }
            return value as! V
        } else if Int.self == V.self {
            guard let value = self[key]?.intValue else { throw SQLiteError.onDecodingRow(key) }
            return value as! V
        } else if Bool.self == V.self {
            guard let value = self[key]?.boolValue else { throw SQLiteError.onDecodingRow(key) }
            return value as! V
        } else if Double.self == V.self {
            guard let value = self[key]?.doubleValue
            else { throw SQLiteError.onDecodingRow(key) }
            return value as! V
        } else if Data.self == V.self {
            guard let value = self[key]?.dataValue else { throw SQLiteError.onDecodingRow(key) }
            return value as! V
        } else if Date.self == V.self {
            guard let date = date(from: self[key]?.stringValue)
            else { throw SQLiteError.onDecodingRow(key) }
            return date as! V
        } else if Int64.self == V.self {
            guard let value = self[key]?.int64Value
            else { throw SQLiteError.onDecodingRow(key) }
            return value as! V
        } else if String?.self == V.self {
            return self[key]?.stringValue as! V
        } else if Int?.self == V.self {
            return self[key]?.intValue as! V
        } else if Bool?.self == V.self {
            return self[key]?.boolValue as! V
        } else if Double?.self == V.self {
            return self[key]?.doubleValue as! V
        } else if Data?.self == V.self {
            return self[key]?.dataValue as! V
        } else if Date?.self == V.self {
            return date(from: self[key]?.stringValue) as! V
        } else if Int64?.self == V.self {
            return self[key]?.int64Value as! V
        } else {
            throw SQLiteError.onInvalidDecodingType(String(describing: V.self))
        }
    }
}

private func date(from string: String?) -> Date? {
    guard let string else { return nil }
    return PreciseDateFormatter.date(from: string)
}
