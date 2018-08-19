import XCTest
@testable import SQLite

class SQLiteCodableTests: XCTestCase {
    var directory: String!
    var path: String!
    var database: SQLite.Database!

    override func setUp() {
        super.setUp()
        directory = temporaryDirectory()
        path = (directory as NSString).appendingPathComponent("test.db")
        createDirectory(at: directory)
        database = try! SQLite.Database(path: path)
        try! database.execute(raw: TestCodableType.createTable)
    }

    override func tearDown() {
        super.tearDown()
        database.close()
        removeDirectory(at: directory)
    }

    func testEncodingWithoutNils() {
        let toEncode = _noNils
        let encoder = SQLite.Encoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: TestCodableType.insert))
        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(TestCodableType.getAll, arguments: [:]))

        XCTAssertEqual(1, results.count)
        _assert(results[0], equals: _noNils)
    }

    func testEncodingWithNils() {
        let toEncode = _nils
        let encoder = SQLite.Encoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: TestCodableType.insert))
        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(TestCodableType.getAll, arguments: [:]))

        XCTAssertEqual(1, results.count)
        _assert(results[0], equals: _nils)
    }

    func testEncodingMultiple() {
        let toEncode = [_noNils, _nils]
        let encoder = SQLite.Encoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: TestCodableType.insert))
        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(TestCodableType.getAll, arguments: [:]))

        XCTAssertEqual(2, results.count)
        _assert(results[0], equals: _noNils)
        _assert(results[1], equals: _nils)
    }

    func testUpsertSingle() {
        let original = _nils
        var updated = original
        updated.uuid = UUID()
        updated.optionalString = "Now it's something"
        updated.optionalDate = Date(timeIntervalSinceReferenceDate: 123456789)
        updated.inner.optionalBool = false

        let encoder = SQLite.Encoder(database)

        XCTAssertNoThrow(try encoder.encode(original, using: TestCodableType.insert))
        XCTAssertNoThrow(try encoder.encode(updated, using: TestCodableType.upsert))

        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(TestCodableType.getAll, arguments: [:]))
        XCTAssertEqual(1, results.count)
        _assert(results[0], equals: updated)
    }

    func testUpsertMultiple() {
        let original1 = _nils
        var updated1 = original1
        updated1.optionalString = "Now it's something"
        updated1.optionalDate = Date(timeIntervalSinceReferenceDate: 123456789)
        updated1.inner.date = Date(timeIntervalSinceReferenceDate: 987654321)

        let original2 = _noNils
        var updated2 = original2
        updated2.url = URL(string: "https://shareup.app/blog/")!
        updated2.optionalString = nil
        updated2.optionalDate = nil
        updated2.inner.optionalBool = nil

        let encoder = SQLite.Encoder(database)

        XCTAssertNoThrow(try encoder.encode([original1, original2], using: TestCodableType.insert))
        XCTAssertNoThrow(try encoder.encode([updated1, updated2], using: TestCodableType.upsert))

        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(TestCodableType.getAll, arguments: [:]))
        XCTAssertEqual(2, results.count)
        _assert(results[0], equals: updated2)
        _assert(results[1], equals: updated1)
    }

    func testDecodingWithoutNils() {
        let toDecode = _noNils
        _insert([toDecode])

        let decoder = SQLite.Decoder(database)
        var decoded: TestCodableType?
        let args: SQLiteArguments = ["id": .integer(Int64(toDecode.id))]
        XCTAssertNoThrow(decoded = try decoder.decode(TestCodableType.self, using: TestCodableType.getByID, arguments: args))

        XCTAssertEqual(toDecode, decoded)
    }

    func testDecodingWithNils() {
        let toDecode = _nils
        _insert([toDecode])

        let decoder = SQLite.Decoder(database)
        var decoded: TestCodableType?
        let args: SQLiteArguments = ["id": .integer(Int64(toDecode.id))]
        XCTAssertNoThrow(decoded = try decoder.decode(TestCodableType.self, using: TestCodableType.getByID, arguments: args))

        XCTAssertEqual(toDecode, decoded)
    }

    func testDecodingMultiple() {
        let toDecode = [_noNils, _nils]
        _insert(toDecode)

        let decoder = SQLite.Decoder(database)
        var decoded: Array<TestCodableType> = []
        XCTAssertNoThrow(decoded = try decoder.decode(
            Array<TestCodableType>.self, using: TestCodableType.getAll, arguments: [:]
        ))

        XCTAssertEqual(toDecode, decoded)
    }
}

extension SQLiteCodableTests {
    fileprivate var _noNils: TestCodableType {
        let uuid = UUID(uuidString: "B25D5458-1F18-4BFB-A188-F1BF1E55F796")!
        let inner = Inner(
            string: "Inner One",
            data: "{ \"id\": 123, \"name\": \"John Appleseed\" }".data(using: .utf8)!,
            date: Date(timeIntervalSince1970: 1),
            optionalBool: true
        )
        return TestCodableType(
            id: 1,
            uuid: uuid,
            string: "one",
            data: "one".data(using: .utf8)!,
            url: URL(string: "https://www.microsoft.com/index.html")!,
            optionalString: "one",
            optionalDate: Date(timeIntervalSince1970: -1),
            inner: inner
        )
    }

    fileprivate var _nils: TestCodableType {
        let uuid = UUID(uuidString: "1E54A649-4EEB-4E4A-BCC8-78AF5C8B2B22")!
        let inner = Inner(
            string: "Inner Two",
            data: Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            date: Date(timeIntervalSince1970: 2),
            optionalBool: nil
        )
        return TestCodableType(
            id: 2,
            uuid: uuid,
            string: "two",
            data: "two".data(using: .utf8)!,
            url: URL(fileURLWithPath: NSTemporaryDirectory()),
            optionalString: nil,
            optionalDate: nil,
            inner: inner
        )
    }
}

extension SQLiteCodableTests {
    fileprivate func _assert(_ row: SQLiteRow, equals expected: TestCodableType) {
        XCTAssertEqual(expected.id, row["id"]?.intValue)
        XCTAssertEqual(expected.string, row["string"]?.stringValue)
        XCTAssertEqual(expected.data, row["data"]?.dataValue)
        XCTAssertEqual(expected.optionalString, row["optional_string"]?.stringValue)

        guard let innerText = row["inner"]?.stringValue else { return XCTFail() }
        guard let innerData = innerText.data(using: .utf8) else { return XCTFail() }

        let decoder = JSONDecoder()
        decoder.dataDecodingStrategy = .base64
        decoder.dateDecodingStrategy = .custom({ (decoder) throws -> Foundation.Date in
            let container = try decoder.singleValueContainer()
            let dateAsString = try container.decode(String.self)
            guard let date = SQLite.DateFormatter.date(from: dateAsString) else {
                throw SQLite.Decoder.Error.invalidDate(dateAsString)
            }
            return date
        })
        let inner = try! decoder.decode(Inner.self, from: innerData)
        XCTAssertEqual(expected.inner, inner)
    }

    fileprivate func _insert(_ toInsert: Array<TestCodableType>) {
        let encoder = SQLite.Encoder(database)
        do {
            try encoder.encode(toInsert, using: TestCodableType.insert)
        } catch let error {
            XCTFail("Could not insert \(toInsert): \(error)")
        }
    }
}

extension SQLiteCodableTests {
    fileprivate func temporaryDirectory() -> String {
        return (NSTemporaryDirectory() as NSString).appendingPathComponent("\(arc4random())")
    }

    fileprivate func createDirectory(at path: String) {
        let fileManager = FileManager()

        do {
            try? fileManager.removeItem(atPath: path)
            try fileManager.createDirectory(atPath: path, withIntermediateDirectories: true)
        } catch let error {
            assertionFailure("Could not create directory at '\(path)': \(error)")
        }
    }

    fileprivate func removeDirectory(at path: String) {
        let fileManager = FileManager()

        do {
            try fileManager.removeItem(atPath: path)
        } catch let error {
            assertionFailure("Could not delete directory at '\(path)': \(error)")
        }
    }
}
