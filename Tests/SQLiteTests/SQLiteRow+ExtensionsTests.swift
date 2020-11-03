import XCTest
import SQLite

class SQLiteRowExtensionsTests: XCTestCase {
    func testBoolValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "false": .integer(0),
            "true": .integer(1),
            "positive": .integer(987654321),
            "negative": .integer(-987654321),
        ]

        let expected: Array<(String, Bool)> = [
            ("false", false),
            ("true", true),
            ("positive", true),
            ("negative", true),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testDataValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "empty": .data(Data()),
            "nonempty": .data("123".data(using: .utf8)!),
        ]

        let expected: Array<(String, Data)> = [
            ("empty", Data()),
            ("nonempty", "123".data(using: .utf8)!),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testDateValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "1970": .text(PreciseDateFormatter.string(from: Date(timeIntervalSince1970: 456))),
            "2001": .text(PreciseDateFormatter.string(from: Date(timeIntervalSinceReferenceDate: 123))),
        ]

        let expected: Array<(String, Date)> = [
            ("1970", Date(timeIntervalSince1970: 456)),
            ("2001", Date(timeIntervalSinceReferenceDate: 123)),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testDoubleValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "zero": .double(0),
            "positive": .double(12345.12345),
            "negative": .double(-12345.12345),
        ]

        let expected: Array<(String, Double)> = [
            ("zero", 0),
            ("positive", 12345.12345),
            ("negative", -12345.12345),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testIntValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "zero": .integer(0),
            "positive": .integer(12345),
            "negative": .integer(-12345),
        ]

        let expected: Array<(String, Int)> = [
            ("zero", 0),
            ("positive", 12345),
            ("negative", -12345),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testInt64ValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "zero": .integer(0),
            "positive": .integer(12345),
            "negative": .integer(-12345),
        ]

        let expected: Array<(String, Int64)> = [
            ("zero", 0),
            ("positive", 12345),
            ("negative", -12345),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testStringValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "empty": .text(""),
            "nonempty": .text("This is not empty"),
        ]

        let expected: Array<(String, String)> = [
            ("empty", ""),
            ("nonempty", "This is not empty"),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalBoolValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "notnull": .integer(0),
            "null": .null,
        ]

        let expected: Array<(String, Optional<Bool>)> = [
            ("notnull", false),
            ("null", nil),
            ("missing", nil),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalDataValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "nonnull": .data("123".data(using: .utf8)!),
            "null": .null,
        ]

        let expected: Array<(String, Data?)> = [
            ("nonnull", "123".data(using: .utf8)!),
            ("null", nil),
            ("missing", nil),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalDateValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "null": .null,
            "1970": .text(PreciseDateFormatter.string(from: Date(timeIntervalSince1970: 456))),
            "2001": .text(PreciseDateFormatter.string(from: Date(timeIntervalSinceReferenceDate: 123))),
        ]

        let expected: Array<(String, Date?)> = [
            ("null", nil),
            ("1970", Date(timeIntervalSince1970: 456)),
            ("2001", Date(timeIntervalSinceReferenceDate: 123)),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalDoubleValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "notnull": .double(123.456),
            "null": .null,
        ]

        let expected: Array<(String, Optional<Double>)> = [
            ("notnull", 123.456),
            ("null", nil),
            ("missing", nil),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalIntValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "notnull": .integer(123),
            "null": .null,
        ]

        let expected: Array<(String, Optional<Int>)> = [
            ("notnull", 123),
            ("null", nil),
            ("missing", nil),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalInt64ValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "notnull": .integer(123),
            "null": .null,
        ]

        let expected: Array<(String, Optional<Int64>)> = [
            ("notnull", Int64(123)),
            ("null", nil),
            ("missing", nil),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalStringValueForKey() throws {
        let values: Dictionary<String, SQLiteValue> = [
            "notnull": .text("This is not null"),
            "null": .null,
        ]

        let expected: Array<(String, Optional<String>)> = [
            ("notnull", "This is not null"),
            ("null", nil),
            ("missing", nil),
        ]

        try expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try values.value(for: key))
            XCTAssertEqual(expectedValue, try values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    static var allTests = [
        ("testBoolValueForKey", testBoolValueForKey),
        ("testDataValueForKey", testDataValueForKey),
        ("testDateValueForKey", testDateValueForKey),
        ("testDoubleValueForKey", testDoubleValueForKey),
        ("testIntValueForKey", testIntValueForKey),
        ("testInt64ValueForKey", testInt64ValueForKey),
        ("testStringValueForKey", testStringValueForKey),
        ("testOptionalBoolValueForKey", testOptionalBoolValueForKey),
        ("testOptionalDataValueForKey", testOptionalDataValueForKey),
        ("testOptionalDateValueForKey", testOptionalDateValueForKey),
        ("testOptionalDoubleValueForKey", testOptionalDoubleValueForKey),
        ("testOptionalIntValueForKey", testOptionalIntValueForKey),
        ("testOptionalInt64ValueForKey", testOptionalInt64ValueForKey),
        ("testOptionalStringValueForKey", testOptionalStringValueForKey),
    ]
}

private struct TestCodingKey: CodingKey {
    var stringValue: String
    var intValue: Int? = nil

    init?(stringValue: String) {
        self.stringValue = stringValue
    }

    init?(intValue: Int) {
        return nil
    }
}
