@testable import SQLite
import XCTest

final class CodableTests: XCTestCase {
    var database: SQLiteDatabase!

    override func setUpWithError() throws {
        try super.setUpWithError()
        database = try SQLiteDatabase()
        try database.execute(raw: CodableType.createTable)
    }

    override func tearDownWithError() throws {
        try super.tearDownWithError()
        try database.close()
    }

    func testEncodingWithoutNils() throws {
        let toEncode = _noNils
        let encoder = SQLiteEncoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: CodableType.insert))
        var results = [SQLiteRow]()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))

        XCTAssertEqual(1, results.count)
        try assert(results[0], equals: _noNils)
    }

    func testEncodingWithNils() throws {
        let toEncode = _nils
        let encoder = SQLiteEncoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: CodableType.insert))
        var results = [SQLiteRow]()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))

        XCTAssertEqual(1, results.count)
        try assert(results[0], equals: _nils)
    }

    func testEncodingMultiple() throws {
        let toEncode = [_noNils, _nils]
        let encoder = SQLiteEncoder(database)
        XCTAssertNoThrow(try encoder.encode(toEncode, using: CodableType.insert))
        var results = [SQLiteRow]()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))

        XCTAssertEqual(2, results.count)
        try assert(results[0], equals: _noNils)
        try assert(results[1], equals: _nils)
    }

    func testUpsertSingle() throws {
        let original = _nils
        var updated = original
        updated.uuid = UUID()
        updated.optionalString = "Now it's something"
        updated.optionalDate = Date(timeIntervalSinceReferenceDate: 123_456_789)
        updated.inner.optionalBool = false

        let encoder = SQLiteEncoder(database)

        XCTAssertNoThrow(try encoder.encode(original, using: CodableType.insert))
        XCTAssertNoThrow(try encoder.encode(updated, using: CodableType.upsert))

        var results = [SQLiteRow]()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))
        XCTAssertEqual(1, results.count)
        try assert(results[0], equals: updated)
    }

    func testUpsertMultiple() throws {
        let original1 = _nils
        var updated1 = original1
        updated1.optionalString = "Now it's something"
        updated1.optionalDate = Date(timeIntervalSinceReferenceDate: 123_456_789)
        updated1.inner.date = Date(timeIntervalSinceReferenceDate: 987_654_321)

        let original2 = _noNils
        var updated2 = original2
        updated2.url = URL(string: "https://shareup.app/blog/")!
        updated2.optionalString = nil
        updated2.optionalDate = nil
        updated2.inner.optionalBool = nil

        let encoder = SQLiteEncoder(database)

        XCTAssertNoThrow(try encoder.encode([original1, original2], using: CodableType.insert))
        XCTAssertNoThrow(try encoder.encode([updated1, updated2], using: CodableType.upsert))

        var results = [SQLiteRow]()
        XCTAssertNoThrow(results = try database.read(CodableType.getAll, arguments: [:]))
        XCTAssertEqual(2, results.count)
        try assert(results[0], equals: updated2)
        try assert(results[1], equals: updated1)
    }

    func testDecodingWithoutNils() throws {
        let toDecode = _noNils
        insert([toDecode])

        let decoder = SQLiteDecoder(database)
        var decoded: CodableType?
        let args: SQLiteArguments = ["id": .integer(Int64(toDecode.id))]
        XCTAssertNoThrow(
            decoded = try decoder.decode(
                CodableType.self,
                using: CodableType.getByID,
                arguments: args
            )
        )

        XCTAssertEqual(toDecode, decoded)
    }

    func testDecodingWithNils() throws {
        let toDecode = _nils
        insert([toDecode])

        let decoder = SQLiteDecoder(database)
        var decoded: CodableType?
        let args: SQLiteArguments = ["id": .integer(Int64(toDecode.id))]
        XCTAssertNoThrow(
            decoded = try decoder.decode(
                CodableType.self,
                using: CodableType.getByID,
                arguments: args
            )
        )

        XCTAssertEqual(toDecode, decoded)
    }

    func testDecodingMultiple() throws {
        let toDecode = [_noNils, _nils]
        insert(toDecode)

        let decoder = SQLiteDecoder(database)
        var decoded: [CodableType] = []
        XCTAssertNoThrow(decoded = try decoder.decode(
            [CodableType].self, using: CodableType.getAll, arguments: [:]
        ))

        XCTAssertEqual(toDecode, decoded)
    }
}

private extension CodableTests {
    var _noNils: CodableType {
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

    var _nils: CodableType {
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
    private func assert(_ row: SQLiteRow, equals expected: CodableType) throws {
        XCTAssertEqual(expected.id, row["id"]?.intValue)
        XCTAssertEqual(expected.string, row["string"]?.stringValue)
        XCTAssertEqual(expected.data, row["data"]?.dataValue)
        XCTAssertEqual(expected.optionalString, row["optional_string"]?.stringValue)

        guard let innerText = row["inner"]?.stringValue else { return XCTFail() }
        guard let innerData = innerText.data(using: .utf8) else { return XCTFail() }

        let decoder = JSONDecoder()
        decoder.dataDecodingStrategy = .base64
        decoder.dateDecodingStrategy = .custom { decoder throws -> Foundation.Date in
            let container = try decoder.singleValueContainer()
            let dateAsString = try container.decode(String.self)
            guard let date = PreciseDateFormatter.date(from: dateAsString) else {
                throw SQLiteDecoder.Error.invalidDate(dateAsString)
            }
            return date
        }
        let inner = try decoder.decode(CodableType.Inner.self, from: innerData)
        XCTAssertEqual(expected.inner, inner)
    }

    private func insert(_ toInsert: [CodableType]) {
        let encoder = SQLiteEncoder(database)
        do {
            try encoder.encode(toInsert, using: CodableType.insert)
        } catch {
            XCTFail("Could not insert \(toInsert): \(error)")
        }
    }
}
