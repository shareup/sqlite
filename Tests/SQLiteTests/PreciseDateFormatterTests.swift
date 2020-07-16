import XCTest
@testable import SQLite

class PreciseDateFormatterTests: XCTestCase {
    func testCurrentDateSerializesAndDeserializes() throws {
        let date = Date()
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    func testUnixTimestampSerializesAndDeserializes() throws {
        let date = Date(timeIntervalSince1970: 1534500993.44331)
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    func testISO8601DateSerializesAndDeserializes() throws {
        guard let date = iso8601.date(from: "2018-08-17T10:22:09.995599") else { return XCTFail() }
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    func testEncodingAndDecodingPreciseDate() throws {
        let date = Date(timeIntervalSince1970: 1534500993.44331)
        let model = Model(date)

        let encoded = try JSONEncoder().encode(model)
        let decoded = try JSONDecoder().decode(Model.self, from: encoded)
        XCTAssertEqual(model, decoded)
    }

    static var allTests = [
        ("testCurrentDateSerializesAndDeserializes", testCurrentDateSerializesAndDeserializes),
        ("testUnixTimestampSerializesAndDeserializes", testUnixTimestampSerializesAndDeserializes),
        ("testISO8601DateSerializesAndDeserializes", testISO8601DateSerializesAndDeserializes),
        ("testEncodingAndDecodingPreciseDate", testEncodingAndDecodingPreciseDate),
    ]
}

private struct Model: Codable, Equatable {
    let date: Date

    init(_ date: Date) {
        self.date = date
    }

    enum CodingKeys: String, CodingKey {
        case date
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.date = try container.decodePreciseDate(forKey: .date)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(preciseDate: self.date, forKey: .date)
    }
}

private let iso8601: DateFormatter = {
    let formatter = DateFormatter()
    formatter.calendar = Calendar(identifier: .iso8601)
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    return formatter
}()
