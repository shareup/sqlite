import Foundation

public final class SQLiteEncoder {
    public enum Error: Swift.Error {
        case invalidType(Any)
        case invalidValue(Any)
        case invalidJSON(Data)
        case transactionFailed
    }

    private let _database: SQLiteDatabase

    public init(_ database: SQLiteDatabase) {
        _database = database
    }

    public func encode(_ value: some Encodable, using sql: SQL) throws {
        let encoder = _SQLiteEncoder()

        if let array = value as? [Encodable] {
            do {
                try _database.inTransaction { db in
                    try array.forEach { (element: Encodable) in
                        try element.encode(to: encoder)
                        try db.write(sql, arguments: encoder.encodedArguments)
                    }
                }
            } catch {
                throw SQLiteEncoder.Error.transactionFailed
            }
        } else if let dictionary = value as? [AnyHashable: Encodable] {
            throw SQLiteEncoder.Error.invalidType(dictionary)
        } else {
            try value.encode(to: encoder)
            try _database.write(sql, arguments: encoder.encodedArguments)
        }
    }
}

private class _SQLiteEncoder: Swift.Encoder {
    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] = [:]

    var encodedArguments: SQLiteArguments { _storage.arguments }

    private let _storage = _KeyedStorage()

    func container<Key>(keyedBy _: Key.Type) -> KeyedEncodingContainer<Key>
        where Key: CodingKey
    {
        _storage.reset()
        return KeyedEncodingContainer(_KeyedContainer<Key>(_storage))
    }

    func unkeyedContainer() -> UnkeyedEncodingContainer {
        fatalError("_SQLiteEncoder doesn't support unkeyed encoding")
    }

    func singleValueContainer() -> SingleValueEncodingContainer {
        fatalError("_SQLiteEncoder doesn't support single value encoding")
    }
}

private struct _KeyedContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
    typealias Key = K
    let codingPath: [CodingKey] = []

    private var _storage: _KeyedStorage

    init(_ storage: _KeyedStorage) {
        _storage = storage
    }

    mutating func encodeNil(forKey key: K) throws {
        _storage[key.stringValue] = .null
    }

    mutating func encode(_ value: Bool, forKey key: K) throws {
        _storage[key.stringValue] = .integer(value ? 1 : 0)
    }

    mutating func encode(_ value: Int, forKey key: K) throws {
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Int8, forKey key: K) throws {
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Int16, forKey key: K) throws {
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Int32, forKey key: K) throws {
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Int64, forKey key: K) throws {
        _storage[key.stringValue] = .integer(value)
    }

    mutating func encode(_ value: UInt, forKey key: K) throws {
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: UInt8, forKey key: K) throws {
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: UInt16, forKey key: K) throws {
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: UInt32, forKey key: K) throws {
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: UInt64, forKey key: K) throws {
        guard value < Int64.max else { throw SQLiteEncoder.Error.invalidValue(value) }
        _storage[key.stringValue] = .integer(Int64(value))
    }

    mutating func encode(_ value: Float, forKey key: K) throws {
        _storage[key.stringValue] = .double(Double(value))
    }

    mutating func encode(_ value: Double, forKey key: K) throws {
        _storage[key.stringValue] = .double(value)
    }

    mutating func encode(_ value: String, forKey key: K) throws {
        _storage[key.stringValue] = .text(value)
    }

    mutating func encode(_ value: Data, forKey key: K) throws {
        _storage[key.stringValue] = .data(value)
    }

    mutating func encode(_ value: Date, forKey key: K) throws {
        let string = PreciseDateFormatter.string(from: value)
        _storage[key.stringValue] = .text(string)
    }

    mutating func encode(_ value: URL, forKey key: K) throws {
        _storage[key.stringValue] = .text(value.absoluteString)
    }

    mutating func encode(_ value: UUID, forKey key: K) throws {
        _storage[key.stringValue] = .text(value.uuidString)
    }

    mutating func encode(_ value: some Encodable, forKey key: K) throws {
        if let data = value as? Data {
            try encode(data, forKey: key)
        } else if let date = value as? Date {
            try encode(date, forKey: key)
        } else if let url = value as? URL {
            try encode(url, forKey: key)
        } else if let uuid = value as? UUID {
            try encode(uuid, forKey: key)
        } else {
            let jsonData = try jsonEncoder.encode(value)
            guard let jsonText = String(data: jsonData, encoding: .utf8) else {
                throw SQLiteEncoder.Error.invalidJSON(jsonData)
            }
            _storage[key.stringValue] = .text(jsonText)
        }
    }

    mutating func nestedContainer<NestedKey>(
        keyedBy _: NestedKey.Type,
        forKey _: K
    ) -> KeyedEncodingContainer<NestedKey> where NestedKey: CodingKey {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    mutating func nestedUnkeyedContainer(forKey _: K) -> UnkeyedEncodingContainer {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    mutating func superEncoder() -> Swift.Encoder {
        fatalError("_KeyedContainer does not support nested containers.")
    }

    mutating func superEncoder(forKey _: K) -> Swift.Encoder {
        fatalError("_KeyedContainer does not support nested containers.")
    }
}

private class _KeyedStorage {
    private var _elements = SQLiteArguments()

    var arguments: SQLiteArguments { _elements }

    func reset() {
        _elements.removeAll(keepingCapacity: true)
    }

    subscript(key: String) -> SQLiteValue? {
        get {
            _elements[key]
        }
        set {
            _elements[key] = newValue
        }
    }
}

private let jsonEncoder: JSONEncoder = {
    let encoder = JSONEncoder()
    encoder.dataEncodingStrategy = .base64
    encoder.dateEncodingStrategy = .custom { date, encoder throws in
        let dateAsString = PreciseDateFormatter.string(from: date)
        var container = encoder.singleValueContainer()
        try container.encode(dateAsString)
    }
    return encoder
}()
