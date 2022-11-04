import Combine
import CombineTestExtensions
@testable import SQLite
import SQLite3
import XCTest

final class SQLiteDatabaseTests: XCTestCase {
    var database: SQLiteDatabase!

    override func setUp() {
        super.setUp()
        database = try! SQLiteDatabase()
    }

    override func tearDownWithError() throws {
        try super.tearDownWithError()
        try database.close()
    }

    func testDatabaseIsCreated() throws {
        try Sandbox.execute { directory in
            let path = directory.appendingPathComponent("test.db").path
            let db = try SQLiteDatabase(path: path)
            XCTAssertTrue(FileManager().fileExists(atPath: path))
            try db.close()
        }
    }

    func testDatabaseConnectionIsOpenedInWALMode() throws {
        let fileManager = FileManager.default
        let one: SQLiteArguments = ["id": .integer(1), "data": .data(Data("one".utf8))]

        try Sandbox.execute { directory in
            let path = directory.appendingPathComponent("test.db").path
            let db = try SQLiteDatabase(path: path)

            try db.write(_createTableWithBlob)
            try db.write(_insertIDAndData, arguments: one)

            XCTAssertTrue(fileManager.fileExists(atPath: path + "-shm"))
            XCTAssertTrue(fileManager.fileExists(atPath: path + "-wal"))

            try db.close()
        }
    }

    func testCloseErrorWhenDatabaseIsClosed() throws {
        try database.close()
        XCTAssertThrowsError(
            try database.execute(raw: _createTableWithBlob),
            "Should have thrown database closed error",
            { XCTAssertEqual(.databaseIsClosed, $0 as? SQLiteError) }
        )
        XCTAssertThrowsError(try database.tables())
    }

    func testReopen() throws {
        let one: SQLiteArguments = ["id": .integer(1), "data": .data(Data("one".utf8))]
        let two: SQLiteArguments = ["id": .integer(2), "data": .data(Data("two".utf8))]
        let three: SQLiteArguments = ["id": .integer(3), "data": .data(Data("three".utf8))]

        try Sandbox.execute { directory in
            let path = directory.appendingPathComponent("test.db").path
            let db = try SQLiteDatabase(path: path)

            try db.write(_createTableWithBlob)
            try db.write(_insertIDAndData, arguments: one)
            try db.close()

            do { try db.write(_insertIDAndData, arguments: two) }
            catch SQLiteError.databaseIsClosed {}

            try db.reopen()
            try db.write(_insertIDAndData, arguments: three)

            let rows = try db.read("SELECT * FROM test;")
            XCTAssertEqual(2, rows.count)
            XCTAssertEqual(one, rows.first)
            XCTAssertEqual(three, rows.last)

            try db.close()
        }
    }

    func testUserVersion() throws {
        XCTAssertEqual(0, database.userVersion)

        database.userVersion = 123
        XCTAssertEqual(123, database.userVersion)
    }

    func testIsForeignKeySupportEnabled() throws {
        XCTAssertFalse(database.isForeignKeySupportEnabled)

        database.isForeignKeySupportEnabled = true
        XCTAssertTrue(database.isForeignKeySupportEnabled)
    }

    func testSupportsJSON() throws {
        XCTAssertTrue(database.supportsJSON)
    }

    func testAutoVacuumMode() throws {
        XCTAssertEqual(.none, database.autoVacuumMode)

        database.autoVacuumMode = .full
        XCTAssertEqual(.full, database.autoVacuumMode)

        database.autoVacuumMode = .incremental
        XCTAssertEqual(.incremental, database.autoVacuumMode)
    }

    func testIncrementalVacuumDoesNotThrowIfModeIsNotIncremental() throws {
        XCTAssertEqual(.none, database.autoVacuumMode)
        try database.incrementalVacuum()
    }

    func testIncrementalVacuum() throws {
        func getPageCount() throws -> Int {
            let result = try database.execute(raw: "PRAGMA page_count;").first
            return try XCTUnwrap(result?["page_count"]?.intValue)
        }

        database.autoVacuumMode = .incremental

        XCTAssertEqual(1, try getPageCount())

        try database.execute(raw: _createTableWithBlob)

        try database.inTransaction { db in
            try (0 ..< 1000).forEach { index in
                let args: SQLiteArguments = [
                    "id": .integer(Int64(index)), "data": .data(_textData),
                ]
                try db.write(_insertIDAndData, arguments: args)
            }
        }

        XCTAssertGreaterThan(try getPageCount(), 3)

        try database.write("DELETE FROM test;", arguments: [:])
        try database.incrementalVacuum()

        XCTAssertEqual(3, try getPageCount())
    }

    func testIncrementalVacuumWithPageCount() throws {
        func getPageCount() throws -> Int {
            let result = try database.execute(raw: "PRAGMA page_count;").first
            return try XCTUnwrap(result?["page_count"]?.intValue)
        }

        database.autoVacuumMode = .incremental

        XCTAssertEqual(1, try getPageCount())

        try database.execute(raw: _createTableWithBlob)

        try database.inTransaction { db in
            try (0 ..< 1000).forEach { index in
                let args: SQLiteArguments = [
                    "id": .integer(Int64(index)), "data": .data(_textData),
                ]
                try db.write(_insertIDAndData, arguments: args)
            }
        }

        let pageCount = try getPageCount()
        XCTAssertGreaterThan(pageCount, 3)

        try database.write("DELETE FROM test;", arguments: [:])
        try database.incrementalVacuum(2)

        XCTAssertEqual(pageCount - 2, try getPageCount())
    }

    func testVacuum() throws {
        func getPageCount() throws -> Int {
            let result = try database.execute(raw: "PRAGMA page_count;").first
            return try XCTUnwrap(result?["page_count"]?.intValue)
        }

        XCTAssertEqual(.none, database.autoVacuumMode)
        XCTAssertEqual(0, try getPageCount())

        try database.execute(raw: _createTableWithBlob)

        try database.inTransaction { db in
            try (0 ..< 1000).forEach { index in
                let args: SQLiteArguments = [
                    "id": .integer(Int64(index)), "data": .data(_textData),
                ]
                try db.write(_insertIDAndData, arguments: args)
            }
        }

        XCTAssertGreaterThan(try getPageCount(), 2)

        try database.write("DELETE FROM test;", arguments: [:])
        try database.vacuum()

        XCTAssertEqual(2, try getPageCount())
    }

    func testCreateTable() throws {
        XCTAssertNoThrow(try database.execute(raw: _createTableWithBlob))
        let tableNames = try database.tables()
        XCTAssertEqual("test", tableNames[0])
    }

    func testTablesAndColumns() throws {
        let createTest2 = """
        CREATE TABLE test2 (
            name TEXT PRIMARY KEY NOT NULL,
            avatar BLOB NOT NULL
        );
        """
        XCTAssertNoThrow(try database.execute(raw: _createTableForTestingUniqueColumns))
        XCTAssertNoThrow(try database.execute(raw: _createUniqueIndexDoubleIndex))
        XCTAssertNoThrow(try database.execute(raw: createTest2))

        let expected = ["test", "test2"]
        XCTAssertEqual(expected, try database.tables())
        XCTAssertEqual(
            ["id1", "uniqueText", "uniqueIndexDouble", "normalDouble"],
            try database.columns(in: "test")
        )
        XCTAssertEqual(["name", "avatar"], try database.columns(in: "test2"))
    }

    func testInsertAndFetchBlob() throws {
        let one: SQLiteArguments = ["id": .integer(123), "data": .data(_textData)]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithBlob))
        XCTAssertNoThrow(try database.write(_insertIDAndData, arguments: one))

        var fetched: [SQLiteRow] = []
        XCTAssertNoThrow(
            fetched = try database
                .read(_selectWhereID, arguments: ["id": .integer(123)])
        )
        XCTAssertEqual(1, fetched.count)
        XCTAssertEqual(one, fetched[0])
    }

    func testInsertAndFetchBlobWithPublisher() throws {
        let one: SQLiteArguments = ["id": .integer(123), "data": .data(_textData)]

        let ex = database.inTransactionPublisher { db -> [SQLiteRow] in
            try db.execute(raw: self._createTableWithBlob)
            try db.write(self._insertIDAndData, arguments: one)
            return try db.read(self._selectWhereID, arguments: ["id": .integer(123)])
        }
        .expectOutput([one], expectToFinish: true)

        wait(for: [ex], timeout: 2)
    }

    func testInsertAndFetchFloatStringAndData() throws {
        let one: SQLiteArguments =
            [
                "id": .integer(1),
                "float": .double(1.23),
                "string": .text("123"),
                "data": .data(_textData),
            ]
        let two: SQLiteArguments =
            [
                "id": .integer(2),
                "float": .double(4.56),
                "string": .text("456"),
                "data": .data(_textData),
            ]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithFloatStringData))
        XCTAssertNoThrow(try database.write(_insertIDFloatStringAndData, arguments: one))
        XCTAssertNoThrow(try database.write(_insertIDFloatStringAndData, arguments: two))

        for (id, target) in [1: one, 2: two] {
            var fetched: [SQLiteRow] = []
            XCTAssertNoThrow(
                fetched = try database.read(
                    _selectWhereID,
                    arguments: ["id": .integer(Int64(id))]
                )
            )
            XCTAssertEqual(1, fetched.count)
            XCTAssertEqual(target, fetched[0])
        }
    }

    func testInsertAndFetchNullableText() throws {
        let one: SQLiteArguments = ["id": .text("not null"), "string": .text("so not null")]
        let two: SQLiteArguments = ["id": .text("null"), "string": .null]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithIDAsStringAndNullableString))
        XCTAssertNoThrow(try database.write(_insertIDAndString, arguments: one))
        XCTAssertNoThrow(try database.write(_insertIDAndString, arguments: two))

        for (id, target) in ["not null": one, "null": two] {
            var fetched: [SQLiteRow] = []
            XCTAssertNoThrow(
                fetched = try database
                    .read(_selectWhereID, arguments: ["id": .text(id)])
            )
            XCTAssertEqual(1, fetched.count)
            XCTAssertEqual(target, fetched[0])
        }
    }

    func testInsertAndFetchSQLiteTransformable() throws {
        let one = Transformable(name: "one", age: 1, jobTitle: "boss")
        let two = Transformable(name: "two", age: 2)

        XCTAssertNoThrow(try database.execute(raw: Transformable.createTable))
        XCTAssertNoThrow(try database.write(Transformable.insert, arguments: one.asArguments))
        XCTAssertNoThrow(try database.write(Transformable.insert, arguments: two.asArguments))

        for (name, target) in ["two": two, "three": nil, "one": one] {
            var fetched: [Transformable] = []
            XCTAssertNoThrow(
                fetched = try database.read(
                    Transformable.fetchByName,
                    arguments: ["name": .text(name)]
                )
            )
            if let target {
                XCTAssertEqual(1, fetched.count)
                XCTAssertEqual(target, fetched[0])
            } else {
                XCTAssertEqual(0, fetched.count)
            }
        }
    }

    func testInsertAndFetchSQLiteTransformableWithPublisher() throws {
        let one = Transformable(name: "one", age: 1, jobTitle: "boss")
        let two = Transformable(name: "two", age: 2)

        let ex = database.inTransactionPublisher { db -> [Transformable] in
            try db.execute(raw: Transformable.createTable)
            try db.write(Transformable.insert, arguments: one.asArguments)
            try db.write(Transformable.insert, arguments: two.asArguments)

            return try ["two", "three", "one"].flatMap { name in
                try db.read(
                    Transformable.fetchByName,
                    arguments: ["name": .text(name)]
                )
            }
        }
        .expectOutput([two, one], expectToFinish: true)

        wait(for: [ex], timeout: 2)
    }

    func testInsertTextIntoTypesafeDataColumnFails() throws {
        let one: SQLiteArguments = ["id": .integer(123), "data": .data(_textData)]
        let two: SQLiteArguments = ["id": .integer(456), "data": .text(_text)]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithTypesafeBlob))
        XCTAssertNoThrow(try database.write(_insertIDAndData, arguments: one))
        XCTAssertThrowsError(try database.write(_insertIDAndData, arguments: two)) { error in
            if case let SQLiteError.onStep(code, _) = error {
                XCTAssertEqual(SQLITE_CONSTRAINT, code)
            } else {
                XCTFail("'\(error)' should be 'Error.onStep'")
            }
        }
    }

    func testInsertNilIntoNonNullDataColumnFails() throws {
        let one: SQLiteArguments = ["id": .integer(123), "data": .null]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithBlob))
        XCTAssertThrowsError(try database.write(_insertIDAndData, arguments: one)) { error in
            if case let SQLiteError.onStep(code, _) = error {
                XCTAssertEqual(SQLITE_CONSTRAINT, code)
            } else {
                XCTFail("'\(error)' should be 'Error.onStep'")
            }
        }
    }

    func testInsertOrReplaceWithSameIDReplacesRows() throws {
        let one: SQLiteArguments = ["id": .text("1"), "string": .text("one")]
        let two: SQLiteArguments = ["id": .text("2"), "string": .text("two")]

        let oneUpdated: SQLiteArguments = ["id": .text("1"), "string": .text("updated")]
        let twoUpdated: SQLiteArguments = ["id": .text("2"), "string": .null]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithIDAsStringAndNullableString))
        XCTAssertNoThrow(try database.write(_insertOrReplaceIDAndString, arguments: one))
        XCTAssertNoThrow(try database.write(_insertOrReplaceIDAndString, arguments: two))

        XCTAssertNoThrow(try database.write(_insertOrReplaceIDAndString, arguments: oneUpdated))
        XCTAssertNoThrow(try database.write(_insertOrReplaceIDAndString, arguments: twoUpdated))

        for (id, target) in ["1": oneUpdated, "2": twoUpdated] {
            var fetched: [SQLiteRow] = []
            XCTAssertNoThrow(
                fetched = try database
                    .read(_selectWhereID, arguments: ["id": .text(id)])
            )
            XCTAssertEqual(1, fetched.count)
            XCTAssertEqual(target, fetched[0])
        }
    }

    func testInsertAndFetchValidJSON() throws {
        guard database.supportsJSON else { return XCTFail() }

        let json = """
        {
            "text": "This is some text",
            "number": 1234.03,
            "array": [
                true,
                false
            ],
            "object": {
                "inner": null
            }
        }
        """

        do {
            let write: SQL = "INSERT INTO test VALUES (:id, json(:string));"
            let read: SQL =
                "SELECT json_extract(string, '$.text') AS text FROM test WHERE id=:id;"

            try database.execute(raw: _createTableWithIDAsStringAndNullableString)
            try database.write(write, arguments: ["id": .text("1"), "string": .text(json)])
            let result = try database.read(read, arguments: ["id": .text("1")])
            XCTAssertEqual(1, result.count)
            XCTAssertEqual(SQLiteValue.text("This is some text"), result[0]["text"])
        } catch {
            XCTFail(String(describing: error))
        }
    }

    func testInsertInvalidJSON() throws {
        guard database.supportsJSON else { return XCTFail() }

        try database.execute(raw: _createTableWithIDAsStringAndNullableString)

        let invalidJSON = "\"text\": What is this supposed to be?"
        let write: SQL = "INSERT INTO test VALUES (:id, json(:string));"
        let args: SQLiteArguments = ["id": .text("1"), "string": .text(invalidJSON)]
        XCTAssertThrowsError(try database.write(write, arguments: args))
    }

    func testInsertFloatStringAndDataInTransaction() throws {
        let one: SQLiteArguments =
            [
                "id": .integer(1),
                "float": .double(1.23),
                "string": .text("123"),
                "data": .data(_textData),
            ]
        let two: SQLiteArguments =
            [
                "id": .integer(2),
                "float": .double(4.56),
                "string": .text("456"),
                "data": .data(_textData),
            ]
        let three: SQLiteArguments =
            [
                "id": .integer(3),
                "float": .double(7.89),
                "string": .text("789"),
                "data": .data(_textData),
            ]
        let four: SQLiteArguments =
            [
                "id": .integer(4),
                "float": .double(0.12),
                "string": .text("012"),
                "data": .data(_textData),
            ]
        let five: SQLiteArguments =
            [
                "id": .integer(5),
                "float": .double(3.45),
                "string": .text("345"),
                "data": .data(_textData),
            ]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithFloatStringData))

        let block = { (db: SQLiteDatabase) in
            for row in [one, two, three, four, five] {
                XCTAssertNoThrow(try db.write(self._insertIDFloatStringAndData, arguments: row))
            }
        }

        XCTAssertNoThrow(try database.inTransaction(block))

        for (id, target) in [1: one, 2: two, 3: three, 4: four, 5: five] {
            var fetched: [SQLiteRow] = []
            XCTAssertNoThrow(
                fetched = try database.read(
                    _selectWhereID,
                    arguments: ["id": .integer(Int64(id))]
                )
            )
            XCTAssertEqual(1, fetched.count)
            XCTAssertEqual(target, fetched[0])
        }
    }

    func testReturnValueFromInTransaction() throws {
        let one: SQLiteArguments = [
            "id": .integer(1), "float": .double(1.23), "string": .text("123"),
            "data": .data(_textData),
        ]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithFloatStringData))
        XCTAssertNoThrow(try database.write(_insertIDFloatStringAndData, arguments: one))

        let row = try database.inTransaction { db in
            try db.read(_selectWhereID, arguments: ["id": .integer(1)]).first
        }

        XCTAssertEqual(row, one)
    }

    func testReturnValueFromInTransactionWithoutTry() throws {
        let one: SQLiteArguments = [
            "id": .integer(1), "float": .double(1.23), "string": .text("123"),
            "data": .data(_textData),
        ]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithFloatStringData))
        XCTAssertNoThrow(try database.write(_insertIDFloatStringAndData, arguments: one))

        let row = database.inTransaction { db in
            try? db.read(_selectWhereID, arguments: ["id": .integer(1)]).first
        }

        XCTAssertEqual(row, one)
    }

    func testInvalidInsertOfBlobInTransactionRollsBack() throws {
        let one: SQLiteArguments = ["id": .integer(1), "data": .data(_textData)]
        let two: SQLiteArguments = ["id": .integer(2)]

        XCTAssertNoThrow(try database.execute(raw: _createTableWithBlob))
        XCTAssertNoThrow(try database.write(_insertIDAndData, arguments: one))

        let block = { try ($0 as SQLiteDatabase).write(self._insertIDAndData, arguments: two) }
        XCTAssertThrowsError(try database.inTransaction(block))

        var fetched: [SQLiteRow] = []
        XCTAssertNoThrow(
            fetched = try database
                .read(_selectWhereID, arguments: ["id": .integer(1)])
        )
        XCTAssertEqual(1, fetched.count)
        XCTAssertEqual(one, fetched[0])
    }

    func testInvalidInsertOfBlobInTransactionRollsBackWithPublisher() throws {
        let one: SQLiteArguments = ["id": .integer(1), "data": .data(_textData)]
        let two: SQLiteArguments = ["id": .integer(2)]

        let ex = database.inTransactionPublisher { db -> [SQLiteRow] in
            try db.execute(raw: self._createTableWithBlob)
            try db.write(self._insertIDAndData, arguments: one)
            try db.write(self._insertIDAndData, arguments: two) // throws
            return try db.read(self._selectWhereID, arguments: ["id": .integer(1)])
        }
        .expectFailure(.onStep(19, "INSERT INTO test VALUES (:id, :data);"))

        wait(for: [ex], timeout: 2)

        XCTAssertEqual([], try database.tables())
    }

    func testInvalidInsertOfBlobInTransactionOnlyRollsBackTransactionWithPublisher() throws {
        let one: SQLiteArguments = ["id": .integer(1), "data": .data(_textData)]
        let two: SQLiteArguments = ["id": .integer(2)]

        try database.execute(raw: _createTableWithBlob)
        try database.write(_insertIDAndData, arguments: one)

        let ex = database.inTransactionPublisher { db -> [SQLiteRow] in
            try db.write(self._insertIDAndData, arguments: two) // throws
            return try db.read(self._selectWhereID, arguments: ["id": .integer(1)])
        }
        .expectFailure(.onStep(19, "INSERT INTO test VALUES (:id, :data);"))

        wait(for: [ex], timeout: 2)

        XCTAssertEqual([one], try database.read(_selectWhereID, arguments: ["id": .integer(1)]))
    }

    func testHasOpenTransactions() throws {
        func arguments(with id: Int) -> SQLiteArguments {
            ["id": .integer(Int64(id)), "data": .data(_textData)]
        }

        XCTAssertNoThrow(try database.execute(raw: _createTableWithBlob))

        try database.inTransaction { db in
            XCTAssertTrue(database.hasOpenTransactions)
            XCTAssertNoThrow(try db.write(_insertIDAndData, arguments: arguments(with: 1)))
        }
        XCTAssertFalse(database.hasOpenTransactions)

        try database.inTransaction { db in
            XCTAssertTrue(db.hasOpenTransactions)
            XCTAssertNoThrow(try db.write(_insertIDAndData, arguments: arguments(with: 2)))
            try db.inTransaction { db in
                XCTAssertTrue(db.hasOpenTransactions)
                XCTAssertNoThrow(try db.write(_insertIDAndData, arguments: arguments(with: 3)))
            }
            XCTAssertTrue(db.hasOpenTransactions)
        }
        XCTAssertFalse(database.hasOpenTransactions)
    }

    func testHasOpenTransactionsWithPublisher() throws {
        func arguments(with id: Int) -> SQLiteArguments {
            ["id": .integer(Int64(id)), "data": .data(_textData)]
        }

        XCTAssertNoThrow(try database.execute(raw: _createTableWithBlob))

        XCTAssertFalse(database.hasOpenTransactions)

        let ex = database.inTransactionPublisher { db -> [SQLiteRow] in
            XCTAssertTrue(db.hasOpenTransactions)

            try db.write(self._insertIDAndData, arguments: arguments(with: 1))
            try db.write(self._insertIDAndData, arguments: arguments(with: 2))

            let one = try db.read(self._selectWhereID, arguments: ["id": .integer(1)])
            let two = try db.read(self._selectWhereID, arguments: ["id": .integer(2)])

            XCTAssertTrue(db.hasOpenTransactions)

            return one + two
        }
        .expectOutput([arguments(with: 1), arguments(with: 2)], expectToFinish: true)

        wait(for: [ex], timeout: 2)
    }
}

private extension SQLiteDatabaseTests {
    var _createTableWithBlob: String {
        """
        CREATE TABLE test (
            id INTEGER PRIMARY KEY NOT NULL,
            data BLOB NOT NULL
        );
        """
    }

    var _createTableWithTypesafeBlob: String {
        """
        CREATE TABLE test (
            id INTEGER NOT NULL PRIMARY KEY,
            data BLOB CHECK(typeof(data) = 'blob')
        );
        """
    }

    var _insertIDAndData: String {
        "INSERT INTO test VALUES (:id, :data);"
    }

    var _createTableForTestingUniqueColumns: String {
        """
        CREATE TABLE test (
            id1 INTEGER PRIMARY KEY NOT NULL,
            uniqueText TEXT NOT NULL UNIQUE,
            uniqueIndexDouble DOUBLE NOT NULL,
            normalDouble DOUBLE NOT NULL
        );
        """
    }

    var _createTableWithTwoPrimaryKeysForTestingUniqueColumns: String {
        """
        CREATE TABLE test (
            id1 INTEGER,
            id2 INTEGER,
            uniqueText TEXT UNIQUE,
            uniqueIndexDouble DOUBLE,
            normalDouble DOUBLE,
            PRIMARY KEY(id1, id2)
        );
        """
    }

    var _createUniqueIndexDoubleIndex: String {
        "CREATE UNIQUE INDEX test_unique_index_double_index ON test (uniqueIndexDouble);"
    }

    var _createTableWithFloatStringData: String {
        """
        CREATE TABLE test (
            id INTEGER PRIMARY KEY NOT NULL,
            float DOUBLE NOT NULL,
            string TEXT NOT NULL,
            data BLOB NOT NULL
        );
        """
    }

    var _insertIDFloatStringAndData: String {
        "INSERT INTO test VALUES (:id, :float, :string, :data);"
    }

    var _createTableWithIDAsStringAndNullableString: String {
        """
        CREATE TABLE test (
            id TEXT PRIMARY KEY NOT NULL,
            string TEXT
        );
        """
    }

    var _insertIDAndString: String {
        "INSERT INTO test VALUES (:id, :string);"
    }

    var _insertOrReplaceIDAndString: String {
        "INSERT OR REPLACE INTO test VALUES (:id, :string);"
    }

    var _selectWhereID: String {
        "SELECT * FROM test WHERE id=:id;"
    }
}

private extension SQLiteDatabaseTests {
    var _text: String {
        "This is a test string! 我们要试一下！👩‍👩‍👧‍👧👮🏿"
    }

    var _textData: Data {
        _text.data(using: .utf8)!
    }
}
