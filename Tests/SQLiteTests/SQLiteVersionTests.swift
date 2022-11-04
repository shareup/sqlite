@testable import SQLite
import XCTest

final class SQLiteVersionTests: XCTestCase {
    func testInitWithRows() throws {
        let valid: [SQLiteRow] = [["version": .text("3.35.5")]]

        XCTAssertEqual(v3_35_5, try SQLiteVersion(rows: valid))
        XCTAssertThrowsError(try SQLiteVersion(rows: [["Version": .text("3.35.5")]]))
        XCTAssertThrowsError(try SQLiteVersion(rows: [["version": .text("35.5")]]))
        XCTAssertThrowsError(try SQLiteVersion(rows: [["version": .text("35")]]))
        XCTAssertThrowsError(try SQLiteVersion(rows: [["version": .text("3.3.35.5")]]))
    }

    func testEquatable() throws {
        XCTAssertEqual(v3_36_0, v3_36_0)
        XCTAssertEqual(v3_35_5, v3_35_5)
        XCTAssertEqual(v3_24_0, v3_24_0)
        XCTAssertEqual(v3_23_1, v3_23_1)

        XCTAssertNotEqual(v4_35_5, v3_35_5)
        XCTAssertNotEqual(v3_36_0, v3_35_5)
        XCTAssertNotEqual(v3_35_5, v3_36_0)
        XCTAssertNotEqual(v3_36_0, v3_24_0)
        XCTAssertNotEqual(v3_36_0, v3_23_1)
    }

    func testComparable() throws {
        XCTAssertTrue(v3_36_0 > v3_35_5)
        XCTAssertFalse(v3_36_0 < v3_35_5)

        XCTAssertFalse(v3_35_5 < v3_35_5)
        XCTAssertTrue(v3_35_5 <= v3_35_5)

        XCTAssertTrue(v3_23_1 > SQLiteVersion(major: 2, minor: 99, patch: 99))
        XCTAssertFalse(v3_23_1 > SQLiteVersion(major: 3, minor: 23, patch: 99))
    }

    func testIsSupported() throws {
        XCTAssertTrue(v3_36_0.isSupported)
        XCTAssertTrue(v3_35_5.isSupported)
        XCTAssertTrue(v3_24_0.isSupported)

        XCTAssertFalse(v4_35_5.isSupported)
        XCTAssertFalse(v3_23_1.isSupported)
        XCTAssertFalse(SQLiteVersion(major: 1, minor: 24, patch: 0).isSupported)
    }
}

private extension SQLiteVersionTests {
    // Unsupported SQLite 4
    var v4_35_5: SQLiteVersion {
        .init(major: 4, minor: 35, patch: 5)
    }

    // Output of `EXPLAIN QUERY PLAN` changed with this version in iOS 15.
    var v3_36_0: SQLiteVersion {
        .init(major: 3, minor: 36, patch: 0)
    }

    // Last version before `EXPLAIN QUERY PLAN` output changes in `3.36.0`
    var v3_35_5: SQLiteVersion {
        .init(major: 3, minor: 35, patch: 5)
    }

    // First supported version
    var v3_24_0: SQLiteVersion {
        .init(major: 3, minor: 24, patch: 0)
    }

    // Unsupported version
    var v3_23_1: SQLiteVersion {
        .init(major: 3, minor: 23, patch: 1)
    }
}
