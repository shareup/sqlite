import CombineTestExtensions
@testable import SQLite
import XCTest

final class SQLiteCrossProcessMonitorTests: XCTestCase {
    func testCanNotOpenASharedInMemoryDatabase() throws {
        XCTAssertThrowsError(
            try SQLiteDatabase.makeShared(path: ":memory:"),
            "Shared in-memory databases can't be created"
        ) { error in
            guard case let .onInvalidPath(path) = (error as! SQLiteError)
            else { return XCTFail() }
            XCTAssertEqual(":memory:", path)
        }
    }

    func testCanCreateSharedDatabase() throws {
        try Sandbox.execute { directory in
            let dbPath = directory.appendingPathComponent("test.db").path
            let db = try SQLiteDatabase.makeShared(path: dbPath)
            try db.write(createTable)
            XCTAssertEqual(["test"], try db.tables())
            try db.close()
        }
    }

    func testIsNotifiedOfChangeFromDifferentConnection() throws {
        try Sandbox.execute { directory in
            let dbPath = directory.appendingPathComponent("test.db").path

            let db1 = try SQLiteDatabase.makeShared(path: dbPath)
            let db2 = try SQLiteDatabase.makeShared(path: dbPath)

            try db1.write(createTable)

            let ex1 = db1
                .publisher(Test.self, getAll)
                .removeDuplicates()
                .expectOutput([[], [Test(1, "one")]])

            let ex2 = db2
                .publisher(Test.self, getAll)
                .removeDuplicates()
                .expectOutput([[], [Test(1, "one")]])

            try db2.write(
                "INSERT INTO test VALUES (:id, :text);",
                arguments: ["id": .integer(1), "text": .text("one")]
            )

            wait(for: [ex1, ex2], timeout: 4) // Coordinated writes can be very slow

            try db1.close()
            try db2.close()
        }
    }

    func testIsNotNotifiedOfChangeFromSameConnection() throws {
        try Sandbox.execute { directory in
            let dbPath = directory.appendingPathComponent("test.db").path
            let db = try SQLiteDatabase(path: dbPath)
            try db.write(createTable)

            var expected: [[Test]] = [[], [Test(1, "one")]]

            let outputEx = expectation(description: "Should have received expected output")
            let duplicateEx = expectation(
                description: "Should not have received duplicate notifications"
            )
            duplicateEx.isInverted = true

            let sub = db
                .publisher(Test.self, getAll, tables: ["test"])
                .sink(
                    receiveCompletion: { _ in },
                    receiveValue: { rows in
                        guard !expected.isEmpty else {
                            duplicateEx.fulfill()
                            return
                        }
                        XCTAssertEqual(expected.removeFirst(), rows)
                        if expected.isEmpty {
                            outputEx.fulfill()
                        }
                    }
                )

            defer { sub.cancel() }

            try db.write(
                "INSERT INTO test VALUES (:id, :text);",
                arguments: ["id": .integer(1), "text": .text("one")]
            )

            wait(for: [outputEx], timeout: 2)
            wait(for: [duplicateEx], timeout: 2) // Coordinated writes can be very slow

            try db.close()
        }
    }
}

private enum Err: Error {
    case couldNotLocateSQLite3
    case sqlite3CommandFailed
}

private extension SQLiteCrossProcessMonitorTests {
    var createTable: SQL {
        """
        CREATE TABLE test (
            id INTEGER PRIMARY KEY NOT NULL,
            text TEXT
        );
        """
    }

    var getAll: SQL {
        "SELECT * FROM test;"
    }

    var insert1: SQL {
        "INSERT INTO test (id, text) VALUES (1, \"one\");"
    }
}

private struct Test: Equatable, SQLiteTransformable {
    let id: Int
    let text: String

    init(_ id: Int, _ text: String) {
        self.id = id
        self.text = text
    }

    init(row: SQLiteRow) throws {
        id = try row.value(for: "id")
        text = try row.value(for: "text")
    }
}
