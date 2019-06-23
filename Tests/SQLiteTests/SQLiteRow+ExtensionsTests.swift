import XCTest
import SQLite

class SQLiteRowExtensionsTests: XCTestCase {
    func testBoolValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
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

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testDataValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "empty": .data(Data()),
            "nonempty": .data("123".data(using: .utf8)!),
        ]

        let expected: Array<(String, Data)> = [
            ("empty", Data()),
            ("nonempty", "123".data(using: .utf8)!),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testDoubleValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "zero": .double(0),
            "positive": .double(12345.12345),
            "negative": .double(-12345.12345),
        ]

        let expected: Array<(String, Double)> = [
            ("zero", 0),
            ("positive", 12345.12345),
            ("negative", -12345.12345),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testIntValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "zero": .integer(0),
            "positive": .integer(12345),
            "negative": .integer(-12345),
        ]

        let expected: Array<(String, Int)> = [
            ("zero", 0),
            ("positive", 12345),
            ("negative", -12345),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testInt64ValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "zero": .integer(0),
            "positive": .integer(12345),
            "negative": .integer(-12345),
        ]

        let expected: Array<(String, Int64)> = [
            ("zero", 0),
            ("positive", 12345),
            ("negative", -12345),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testStringValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "empty": .text(""),
            "nonempty": .text("This is not empty"),
        ]

        let expected: Array<(String, String)> = [
            ("empty", ""),
            ("nonempty", "This is not empty"),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalBoolValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "notnull": .integer(0),
            "null": .null,
        ]

        let expected: Array<(String, Optional<Bool>)> = [
            ("notnull", false),
            ("null", nil),
            ("missing", nil),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalDataValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "nonnull": .data("123".data(using: .utf8)!),
            "null": .null,
        ]

        let expected: Array<(String, Data?)> = [
            ("nonnull", "123".data(using: .utf8)!),
            ("null", nil),
            ("missing", nil),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalDoubleValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "notnull": .double(123.456),
            "null": .null,
        ]

        let expected: Array<(String, Optional<Double>)> = [
            ("notnull", 123.456),
            ("null", nil),
            ("missing", nil),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalIntValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "notnull": .integer(123),
            "null": .null,
        ]

        let expected: Array<(String, Optional<Int>)> = [
            ("notnull", 123),
            ("null", nil),
            ("missing", nil),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalInt64ValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "notnull": .integer(123),
            "null": .null,
        ]

        let expected: Array<(String, Optional<Int64>)> = [
            ("notnull", Int64(123)),
            ("null", nil),
            ("missing", nil),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }

    func testOptionalStringValueForKey() {
        let values: Dictionary<String, SQLite.Value> = [
            "notnull": .text("This is not null"),
            "null": .null,
        ]

        let expected: Array<(String, Optional<String>)> = [
            ("notnull", "This is not null"),
            ("null", nil),
            ("missing", nil),
        ]

        expected.forEach { (key, expectedValue) in
            XCTAssertEqual(expectedValue, try! values.value(for: key))
            XCTAssertEqual(expectedValue, try! values.value(for: TestCodingKey(stringValue: key)!))
        }
    }
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
