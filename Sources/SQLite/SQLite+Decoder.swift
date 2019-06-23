import Foundation

extension SQLite {
    public final class Decoder {
        public enum Error: Swift.Error {
            case incorrectNumberOfResults(Int)
            case missingValueForKey(String)
            case invalidDate(String)
            case invalidURL(String)
            case invalidUUID(String)
            case invalidJSON(String)
        }

        private let _database: SQLite.Database

        public init(_ database: SQLite.Database) {
            _database = database
        }

        public func decode<T: Decodable>(_ type: T.Type, using sql: SQL,
                                         arguments: SQLiteArguments = [:]) throws -> T? {
            let results: Array<T> = try self.decode(Array<T>.self, using: sql, arguments: arguments)
            guard results.count == 0 || results.count == 1 else {
                throw SQLite.Decoder.Error.incorrectNumberOfResults(results.count)
            }
            return results.first
        }

        public func decode<T: Decodable>(_ type: Array<T>.Type, using sql: SQL,
                                         arguments: SQLiteArguments = [:]) throws -> Array<T> {
            let results: Array<SQLiteRow> = try _database.read(sql, arguments: arguments)
            let decoder = _SQLiteDecoder()
            return try results.map { (row: SQLiteRow) in
                decoder.row = row
                return try T.init(from: decoder)
            }
        }
    }
}

private class _SQLiteDecoder: Decoder {
    var codingPath: Array<CodingKey> = []
    var userInfo: Dictionary<CodingUserInfoKey, Any> = [:]

    var row: SQLiteRow?

    init(_ row: SQLiteRow? = nil) {
        self.row = row
    }

    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
        guard let row = self.row else { fatalError() }
        return KeyedDecodingContainer(_KeyedContainer<Key>(row))
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        fatalError("SQLiteDecoder doesn't support unkeyed decoding")
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        fatalError("SQLiteDecoder doesn't support single value decoding")
    }
}

private class _KeyedContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    typealias Key = K

    let codingPath: Array<CodingKey> = []
    var allKeys: Array<K> { return _row.keys.compactMap { K(stringValue: $0) } }

    private var _row: SQLiteRow

    init(_ row: SQLiteRow) {
        _row = row
    }

    func contains(_ key: K) -> Bool {
        return _row[key.stringValue] != nil
    }

    func decodeNil(forKey key: K) throws -> Bool {
        guard let value = _row[key.stringValue] else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }

        if case .null = value {
            return true
        } else {
            return false
        }
    }

    func decode(_ type: Bool.Type, forKey key: K) throws -> Bool {
        guard let value = _row[key.stringValue]?.boolValue else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_ type: Int.Type, forKey key: K) throws -> Int {
        guard let value = _row[key.stringValue]?.intValue else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_ type: Int8.Type, forKey key: K) throws -> Int8 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return Int8(value)
    }

    func decode(_ type: Int16.Type, forKey key: K) throws -> Int16 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return Int16(value)
    }

    func decode(_ type: Int32.Type, forKey key: K) throws -> Int32 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return Int32(value)
    }

    func decode(_ type: Int64.Type, forKey key: K) throws -> Int64 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_ type: UInt.Type, forKey key: K) throws -> UInt {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt(value)
    }

    func decode(_ type: UInt8.Type, forKey key: K) throws -> UInt8 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt8(value)
    }

    func decode(_ type: UInt16.Type, forKey key: K) throws -> UInt16 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt16(value)
    }

    func decode(_ type: UInt32.Type, forKey key: K) throws -> UInt32 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt32(value)
    }

    func decode(_ type: UInt64.Type, forKey key: K) throws -> UInt64 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt64(value)
    }

    func decode(_ type: Float.Type, forKey key: K) throws -> Float {
        guard let value = _row[key.stringValue]?.doubleValue else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return Float(value)
    }

    func decode(_ type: Double.Type, forKey key: K) throws -> Double {
        guard let value = _row[key.stringValue]?.doubleValue else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_ type: String.Type, forKey key: K) throws -> String {
        guard let value = _row[key.stringValue]?.stringValue else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_ type: Data.Type, forKey key: K) throws -> Data {
        guard let value = _row[key.stringValue]?.dataValue else {
            throw SQLite.Decoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_ type: Date.Type, forKey key: K) throws -> Date {
        let string = try decode(String.self, forKey: key)
        if let date = SQLite.DateFormatter.date(from: string) {
            return date
        } else {
            throw SQLite.Decoder.Error.invalidDate(string)
        }
    }

    func decode(_ type: URL.Type, forKey key: K) throws -> URL {
        let string = try decode(String.self, forKey: key)
        if let url = URL(string: string) {
            return url
        } else {
            throw SQLite.Decoder.Error.invalidURL(string)
        }
    }

    func decode(_ type: UUID.Type, forKey key: K) throws -> UUID {
        let string = try decode(String.self, forKey: key)
        if let uuid = UUID(uuidString: string) {
            return uuid
        } else {
            throw SQLite.Decoder.Error.invalidUUID(string)
        }
    }

    func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T: Decodable {
        if Data.self == T.self {
            return try decode(Data.self, forKey: key) as! T
        } else if Date.self == T.self {
            return try decode(Date.self, forKey: key) as! T
        } else if URL.self == T.self {
            return try decode(URL.self, forKey: key) as! T
        } else if UUID.self == T.self {
            return try decode(UUID.self, forKey: key) as! T
        } else {
            let jsonText = try decode(String.self, forKey: key)
            guard let jsonData = jsonText.data(using: .utf8) else {
                throw SQLite.Decoder.Error.invalidJSON(jsonText)
            }
            return try jsonDecoder.decode(T.self, from: jsonData)
        }
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    func superDecoder() throws -> Decoder {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    func superDecoder(forKey key: K) throws -> Decoder {
        fatalError("_KeyedContainer does not support nested containers.")
    }
}

private let jsonDecoder: JSONDecoder = {
    let decoder = JSONDecoder()
    decoder.dataDecodingStrategy = .base64
    decoder.dateDecodingStrategy = .custom({ (decoder) throws -> Date in
        let container = try decoder.singleValueContainer()
        let dateAsString = try container.decode(String.self)
        guard let date = SQLite.DateFormatter.date(from: dateAsString) else {
            throw SQLite.Decoder.Error.invalidDate(dateAsString)
        }
        return date
    })
    return decoder
}()
