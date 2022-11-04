import SQLite
import XCTest

final class SQLiteValueTests: XCTestCase {
    func testBinaryIntegerSQLiteValue() throws {
        let int: Int = -123
        let uint: UInt = 123
        let int8: Int8 = -123
        let uint8: UInt8 = 255

        XCTAssertEqual(.integer(-123), int.sqliteValue)
        XCTAssertEqual(.integer(123), uint.sqliteValue)
        XCTAssertEqual(.integer(-123), int8.sqliteValue)
        XCTAssertEqual(.integer(255), uint8.sqliteValue)
    }

    func testBoolSQLiteValue() throws {
        let yes = true
        let no = false

        XCTAssertEqual(.integer(1), yes.sqliteValue)
        XCTAssertEqual(.integer(0), no.sqliteValue)
    }

    func testDataSQLiteValue() throws {
        let bytes: [UInt8] = Array("👋👪".utf8)
        let data = Data(bytes)
        XCTAssertEqual(.data(data), bytes.sqliteValue)
        XCTAssertEqual(.data(data), data.sqliteValue)
    }

    func testDateSQLiteValue() throws {
        let april_26_2021 = DateComponents(
            calendar: Calendar(identifier: .iso8601),
            timeZone: TimeZone(secondsFromGMT: 0),
            year: 2021,
            month: 4,
            day: 26
        )

        let date = try XCTUnwrap(april_26_2021.date)
        let dateAsString = PreciseDateFormatter.string(from: date)
        XCTAssertEqual(.text(dateAsString), date.sqliteValue)
    }

    func testStringSQLiteValue() throws {
        let text = "👋👪"
        XCTAssertEqual(.text(text), text.sqliteValue)
    }

    func testOptionalBinaryIntegerSQLiteValue() throws {
        let int: Int? = -123
        let uint: UInt? = 123
        let int8: Int8? = -123
        let uint8: UInt8? = 255
        let optional: Int32? = nil

        XCTAssertEqual(.integer(-123), int.sqliteValue)
        XCTAssertEqual(.integer(123), uint.sqliteValue)
        XCTAssertEqual(.integer(-123), int8.sqliteValue)
        XCTAssertEqual(.integer(255), uint8.sqliteValue)
        XCTAssertEqual(.null, optional.sqliteValue)
    }

    func testOptionalBoolSQLiteValue() throws {
        let yes: Bool? = true
        let no: Bool? = false
        let optional: Bool? = nil

        XCTAssertEqual(.integer(1), yes.sqliteValue)
        XCTAssertEqual(.integer(0), no.sqliteValue)
        XCTAssertEqual(.null, optional.sqliteValue)
    }

    func testOptionalDataSQLiteValue() throws {
        let bytes: [UInt8]? = Array("👋👪".utf8)
        let data: Data? = Data(bytes!)
        let optional: Data? = nil

        XCTAssertEqual(.data(data!), bytes.sqliteValue)
        XCTAssertEqual(.data(data!), data.sqliteValue)
        XCTAssertEqual(.null, optional.sqliteValue)
    }

    func testOptionalDateSQLiteValue() throws {
        let april_26_2021 = DateComponents(
            calendar: Calendar(identifier: .iso8601),
            timeZone: TimeZone(secondsFromGMT: 0),
            year: 2021,
            month: 4,
            day: 26
        )

        let date: Date? = try XCTUnwrap(april_26_2021.date)
        let dateAsString = PreciseDateFormatter.string(from: date!)
        let optional: Date? = nil

        XCTAssertEqual(.text(dateAsString), date.sqliteValue)
        XCTAssertEqual(.null, optional.sqliteValue)
    }

    func testOptionalStringSQLiteValue() throws {
        let text: String? = "👋👪"
        let optional: String? = nil

        XCTAssertEqual(.text(text!), text.sqliteValue)
        XCTAssertEqual(.null, optional.sqliteValue)
    }
}
