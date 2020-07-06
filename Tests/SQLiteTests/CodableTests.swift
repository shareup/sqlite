import XCTest
@testable import SQLite

class CodableTests: XCTestCase {
    var database: Database!

    override func setUp() {
        super.setUp()
        database = try! Database(path: ":memory:")
        try! database.execute(raw: CodableType.createTable)
    }

    override func tearDown() {
        super.tearDown()
        database.close()
    }

    func testEncodingWithoutNils() {
        let toEncode = _noNils
        let encoder = Encoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: CodableType.insert))
        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))

        XCTAssertEqual(1, results.count)
        assert(results[0], equals: _noNils)
    }

    func testEncodingWithNils() {
        let toEncode = _nils
        let encoder = Encoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: CodableType.insert))
        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))

        XCTAssertEqual(1, results.count)
        assert(results[0], equals: _nils)
    }

    func testEncodingMultiple() {
        let toEncode = [_noNils, _nils]
        let encoder = Encoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: CodableType.insert))
        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))

        XCTAssertEqual(2, results.count)
        assert(results[0], equals: _noNils)
        assert(results[1], equals: _nils)
    }

    func testUpsertSingle() {
        let original = _nils
        var updated = original
        updated.uuid = UUID()
        updated.optionalString = "Now it's something"
        updated.optionalDate = Date(timeIntervalSinceReferenceDate: 123456789)
        updated.inner.optionalBool = false

        let encoder = Encoder(database)

        XCTAssertNoThrow(try encoder.encode(original, using: CodableType.insert))
        XCTAssertNoThrow(try encoder.encode(updated, using: CodableType.upsert))

        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))
        XCTAssertEqual(1, results.count)
        assert(results[0], equals: updated)
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

        let encoder = Encoder(database)

        XCTAssertNoThrow(try encoder.encode([original1, original2], using: CodableType.insert))
        XCTAssertNoThrow(try encoder.encode([updated1, updated2], using: CodableType.upsert))

        var results = Array<SQLiteRow>()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))
        XCTAssertEqual(2, results.count)
        assert(results[0], equals: updated2)
        assert(results[1], equals: updated1)
    }

    func testDecodingWithoutNils() {
        let toDecode = _noNils
        insert([toDecode])

        let decoder = Decoder(database)
        var decoded: CodableType?
        let args: SQLiteArguments = ["id": .integer(Int64(toDecode.id))]
        XCTAssertNoThrow(decoded = try decoder.decode(CodableType.self, using: CodableType.getByID, arguments: args))

        XCTAssertEqual(toDecode, decoded)
    }

    func testDecodingWithNils() {
        let toDecode = _nils
        insert([toDecode])

        let decoder = Decoder(database)
        var decoded: CodableType?
        let args: SQLiteArguments = ["id": .integer(Int64(toDecode.id))]
        XCTAssertNoThrow(decoded = try decoder.decode(CodableType.self, using: CodableType.getByID, arguments: args))

        XCTAssertEqual(toDecode, decoded)
    }

    func testDecodingMultiple() {
        let toDecode = [_noNils, _nils]
        insert(toDecode)

        let decoder = Decoder(database)
        var decoded: Array<CodableType> = []
        XCTAssertNoThrow(decoded = try decoder.decode(
            Array<CodableType>.self, using: CodableType.getAll, arguments: [:]
            ))

        XCTAssertEqual(toDecode, decoded)
    }

    static var allTests = [
        ("testEncodingWithoutNils", testEncodingWithoutNils),
        ("testEncodingWithNils", testEncodingWithNils),
        ("testEncodingMultiple", testEncodingMultiple),
        ("testUpsertSingle", testUpsertSingle),
        ("testUpsertMultiple", testUpsertMultiple),
        ("testDecodingWithoutNils", testDecodingWithoutNils),
        ("testDecodingWithNils", testDecodingWithNils),
        ("testDecodingMultiple", testDecodingMultiple),
    ]
}

extension CodableTests {
    fileprivate var _noNils: CodableType {
        let uuid = UUID(uuidString: "B25D5458-1F18-4BFB-A188-F1BF1E55F796")!
        let inner = CodableType.Inner(
            string: "Inner One",
            data: "{ \"id\": 123, \"name\": \"John Appleseed\" }".data(using: .utf8)!,
            date: Date(timeIntervalSince1970: 1),
            optionalBool: true
        )
        return CodableType(
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

    fileprivate var _nils: CodableType {
        let uuid = UUID(uuidString: "1E54A649-4EEB-4E4A-BCC8-78AF5C8B2B22")!
        let inner = CodableType.Inner(
            string: "Inner Two",
            data: Data([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            date: Date(timeIntervalSince1970: 2),
            optionalBool: nil
        )
        return CodableType(
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

extension CodableTests {
    private func assert(_ row: SQLiteRow, equals expected: CodableType) {
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
            guard let date = PreciseDateFormatter.date(from: dateAsString) else {
                throw Decoder.Error.invalidDate(dateAsString)
            }
            return date
        })
        let inner = try! decoder.decode(CodableType.Inner.self, from: innerData)
        XCTAssertEqual(expected.inner, inner)
    }

    private func insert(_ toInsert: Array<CodableType>) {
        let encoder = Encoder(database)
        do {
            try encoder.encode(toInsert, using: CodableType.insert)
        } catch let error {
            XCTFail("Could not insert \(toInsert): \(error)")
        }
    }
}
