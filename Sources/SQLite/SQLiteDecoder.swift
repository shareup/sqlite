import Foundation

public final class SQLiteDecoder {
    public enum Error: Swift.Error {
        case incorrectNumberOfResults(Int)
        case missingValueForKey(String)
        case invalidDate(String)
        case invalidURL(String)
        case invalidUUID(String)
        case invalidJSON(String)
    }

    private let _database: SQLiteDatabase

    public init(_ database: SQLiteDatabase) {
        _database = database
    }

    public func decode<T: Decodable>(
        _: T.Type,
        using sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws -> T? {
        let results: [T] = try decode([T].self, using: sql, arguments: arguments)
        guard results.count == 0 || results.count == 1 else {
            throw SQLiteDecoder.Error.incorrectNumberOfResults(results.count)
        }
        return results.first
    }

    public func decode<T: Decodable>(
        _: [T].Type,
        using sql: SQL,
        arguments: SQLiteArguments = [:]
    ) throws -> [T] {
        let results: [SQLiteRow] = try _database.read(sql, arguments: arguments)
        let decoder = _SQLiteDecoder()
        return try results.map { (row: SQLiteRow) in
            decoder.row = row
            return try T(from: decoder)
        }
    }
}

private class _SQLiteDecoder: Swift.Decoder {
    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] = [:]

    var row: SQLiteRow?

    init(_ row: SQLiteRow? = nil) {
        self.row = row
    }

    func container<Key>(keyedBy _: Key.Type) throws -> KeyedDecodingContainer<Key>
        where Key: CodingKey
    {
        guard let row else { fatalError() }
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

    let codingPath: [CodingKey] = []
    var allKeys: [K] { _row.keys.compactMap { K(stringValue: $0) } }

    private var _row: SQLiteRow

    init(_ row: SQLiteRow) {
        _row = row
    }

    func contains(_ key: K) -> Bool {
        _row[key.stringValue] != nil
    }

    func decodeNil(forKey key: K) throws -> Bool {
        guard let value = _row[key.stringValue] else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }

        if case .null = value {
            return true
        } else {
            return false
        }
    }

    func decode(_: Bool.Type, forKey key: K) throws -> Bool {
        guard let value = _row[key.stringValue]?.boolValue else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_: Int.Type, forKey key: K) throws -> Int {
        guard let value = _row[key.stringValue]?.intValue else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_: Int8.Type, forKey key: K) throws -> Int8 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return Int8(value)
    }

    func decode(_: Int16.Type, forKey key: K) throws -> Int16 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return Int16(value)
    }

    func decode(_: Int32.Type, forKey key: K) throws -> Int32 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return Int32(value)
    }

    func decode(_: Int64.Type, forKey key: K) throws -> Int64 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_: UInt.Type, forKey key: K) throws -> UInt {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt(value)
    }

    func decode(_: UInt8.Type, forKey key: K) throws -> UInt8 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt8(value)
    }

    func decode(_: UInt16.Type, forKey key: K) throws -> UInt16 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt16(value)
    }

    func decode(_: UInt32.Type, forKey key: K) throws -> UInt32 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt32(value)
    }

    func decode(_: UInt64.Type, forKey key: K) throws -> UInt64 {
        guard let value = _row[key.stringValue]?.int64Value else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return UInt64(value)
    }

    func decode(_: Float.Type, forKey key: K) throws -> Float {
        guard let value = _row[key.stringValue]?.doubleValue else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return Float(value)
    }

    func decode(_: Double.Type, forKey key: K) throws -> Double {
        guard let value = _row[key.stringValue]?.doubleValue else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_: String.Type, forKey key: K) throws -> String {
        guard let value = _row[key.stringValue]?.stringValue else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_: Data.Type, forKey key: K) throws -> Data {
        guard let value = _row[key.stringValue]?.dataValue else {
            throw SQLiteDecoder.Error.missingValueForKey(key.stringValue)
        }
        return value
    }

    func decode(_: Date.Type, forKey key: K) throws -> Date {
        let string = try decode(String.self, forKey: key)
        if let date = PreciseDateFormatter.date(from: string) {
            return date
        } else {
            throw SQLiteDecoder.Error.invalidDate(string)
        }
    }

    func decode(_: URL.Type, forKey key: K) throws -> URL {
        let string = try decode(String.self, forKey: key)
        if let url = URL(string: string) {
            return url
        } else {
            throw SQLiteDecoder.Error.invalidURL(string)
        }
    }

    func decode(_: UUID.Type, forKey key: K) throws -> UUID {
        let string = try decode(String.self, forKey: key)
        if let uuid = UUID(uuidString: string) {
            return uuid
        } else {
            throw SQLiteDecoder.Error.invalidUUID(string)
        }
    }

    func decode<T>(_: T.Type, forKey key: K) throws -> T where T: Decodable {
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
                throw SQLiteDecoder.Error.invalidJSON(jsonText)
            }
            return try jsonDecoder.decode(T.self, from: jsonData)
        }
    }

    func nestedContainer<NestedKey>(
        keyedBy _: NestedKey.Type,
        forKey _: K
    ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    func nestedUnkeyedContainer(forKey _: K) throws -> UnkeyedDecodingContainer {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    func superDecoder() throws -> Swift.Decoder {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    func superDecoder(forKey _: K) throws -> Swift.Decoder {
        fatalError("_KeyedContainer does not support nested containers.")
    }
}

private let jsonDecoder: JSONDecoder = {
    let decoder = JSONDecoder()
    decoder.dataDecodingStrategy = .base64
    decoder.dateDecodingStrategy = .custom { decoder throws -> Date in
        let container = try decoder.singleValueContainer()
        let dateAsString = try container.decode(String.self)
        guard let date = PreciseDateFormatter.date(from: dateAsString) else {
            throw SQLiteDecoder.Error.invalidDate(dateAsString)
        }
        return date
    }
    return decoder
}()
