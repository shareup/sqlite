import PreciseISO8601DateFormatter
@testable import SQLite
import XCTest

final class PreciseDateFormatterTests: XCTestCase {
    func testCurrentDateSerializesAndDeserializes() throws {
        let date = Date()
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    func testUnixTimestampSerializesAndDeserializes() throws {
        let date = Date(timeIntervalSince1970: 1_534_500_993.44331)
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    func testISO8601DateSerializesAndDeserializes() throws {
        let formatter = PreciseISO8601DateFormatter()
        guard let date = formatter.date(from: "2018-08-17T10:22:09.995599Z")
        else { return XCTFail() }
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    func testEncodingAndDecodingPreciseDate() throws {
        let date = Date(timeIntervalSince1970: 1_534_500_993.44331)
        let optionalSome = Date(timeIntervalSince1970: 1)
        let optionalNone: Date? = nil

        let one = Model(date, optionalSome)
        let two = Model(date, optionalNone)

        for model in [one, two] {
            let encoded = try JSONEncoder().encode(model)
            let decoded = try JSONDecoder().decode(Model.self, from: encoded)
            XCTAssertEqual(model, decoded)
        }
    }
}

private struct Model: Codable, Equatable {
    let date: Date
    let optionalDate: Date?

    init(_ date: Date, _ optionalDate: Date?) {
        self.date = date
        self.optionalDate = optionalDate
    }

    enum CodingKeys: String, CodingKey {
        case date
        case optionalDate
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        date = try container.decodePreciseDate(forKey: .date)
        optionalDate = try container.decodePreciseDateIfPresent(forKey: .optionalDate)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(preciseDate: date, forKey: .date)
        try container.encodeIfPresent(preciseDate: optionalDate, forKey: .optionalDate)
    }
}
