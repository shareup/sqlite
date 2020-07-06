import XCTest
@testable import SQLite

class PreciseDateFormatterTests: XCTestCase {
    func testCurrentDateSerializesAndDeserializes() {
        let date = Date()
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    func testUnixTimestampSerializesAndDeserializes() {
        let date = Date(timeIntervalSince1970: 1534500993.44331)
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    func testISO8601DateSerializesAndDeserializes() {
        guard let date = iso8601.date(from: "2018-08-17T10:22:09.995599") else { return XCTFail() }
        let dateAsString = SQLite.PreciseDateFormatter.string(from: date)
        let dateFromString = SQLite.PreciseDateFormatter.date(from: dateAsString)
        XCTAssertNotNil(dateFromString)
        XCTAssertEqual(date, dateFromString)
    }

    static var allTests = [
        ("testCurrentDateSerializesAndDeserializes", testCurrentDateSerializesAndDeserializes),
        ("testUnixTimestampSerializesAndDeserializes", testUnixTimestampSerializesAndDeserializes),
        ("testISO8601DateSerializesAndDeserializes", testISO8601DateSerializesAndDeserializes),
    ]
}

private let iso8601: DateFormatter = {
    let formatter = DateFormatter()
    formatter.calendar = Calendar(identifier: .iso8601)
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    return formatter
}()
