import XCTest
import CombineTestExtensions
@testable import SQLite

class SQLiteCrossProcessMonitorTests: XCTestCase {
    #if os(macOS)
    func testIsNotifiedOfChangeFromDifferentProcess() throws {
        try Sandbox.execute { (directory) in
            let dbPath = directory.appendingPathComponent("test.db").path
            let db = try SQLiteDatabase(path: dbPath)
            try db.write(createTable)

            let ex = db
                .publisher(Test.self, getAll)
                .expectOutput([[], [Test(1, "one")]])

            try executeInDifferentProcess(sql: insert1, databasePath: dbPath)

            wait(for: [ex], timeout: 4) // Coordinated writes are very slow
        }
    }
    #endif

    func testIsNotNotifiedOfChangeFromSameProcess() throws {
        try Sandbox.execute { (directory) in
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
                .publisher(Test.self, getAll)
                .sink(
                    receiveCompletion: { _ in  },
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
            wait(for: [duplicateEx], timeout: 2) // Coordinated writes are very slow.
        }
    }
}

private extension SQLiteCrossProcessMonitorTests {
    #if os(macOS)
    func executeInDifferentProcess(sql: SQL, databasePath: String) throws {
        let sqlite3 = URL(fileURLWithPath: "/usr/bin/sqlite3")

        var terminationStatus: Int32 = -1

        let coordinator = NSFileCoordinator()
        coordinator.coordinate(
            writingItemAt: URL(fileURLWithPath: databasePath + "-change-tracker"),
            options: .forReplacing,
            error: nil
        ) { (url) in
            let process = try! Process.run(sqlite3, arguments: [databasePath, sql])
            process.waitUntilExit()
            terminationStatus = process.terminationStatus
            try! UUID().uuidString.write(to: url, atomically: true, encoding: .utf8)
        }

        guard terminationStatus == 0 else { throw Err.sqlite3CommandFailed }
    }
    #endif
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
