import XCTest
import SQLite3
@testable import SQLite

class QueryPlanParserTests: XCTestCase {
    func testColumnsFromSingleTables() {
        let tables = ["conversations", "TABLE", "SCAN"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([(1, 0, 0, "SCAN TABLE conversations")])
        let expected: Set<String> = ["conversations"]
        let actual = SQLite.QueryPlanParser.tables(in: queryPlan, matching: tables)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsFromMultipleTables() {
        let tables = ["✌🏼 table", "first table", "sqlite_autoindex_✌🏼 table_1", "USING"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (5, 0, 0, "SEARCH TABLE ✌🏼 table USING INDEX sqlite_autoindex_✌🏼 table_1 (id column=?)"),
            (4, 0, 0, "SEARCH TABLE first table USING INDEX sqlite_autoindex_first table_1 (id column=?)")
        ])
        let expected: Set<String> = ["first table", "✌🏼 table"]
        let actual = SQLite.QueryPlanParser.tables(in: queryPlan, matching: tables)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsWithMergesAndJoins() {
        let tables = ["text_messages", "patients", "providers", "AS", "|||"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (1, 0, 0, "MERGE (UNION ALL)"),
            (3, 1, 0, "LEFT"),
            (10, 3, 0, "SEARCH TABLE text_messages USING INDEX text_messages_index"),
            (18, 3, 0, "SCAN TABLE patients"),
            (27, 3, 0, "SCAN TABLE json_each AS USING VIRTUAL TABLE INDEX 1:"),
            (52, 1, 0, "RIGHT"),
            (62, 52, 0, "SEARCH TABLE text_messages USING CONVERING INDEX sqlite_autoindex_1"),
            (66, 52, 0, "SCAN TABLE json_each AS USING VIRTUAL TABLE INDEX 1:"),
        ])
        let expected: Set<String> = ["text_messages", "patients"]
        let actual = SQLite.QueryPlanParser.tables(in: queryPlan, matching: tables)
        XCTAssertEqual(expected, actual)
    }
}

extension QueryPlanParserTests {
    private func queryPlan(_ rows: Array<(Int, Int, Int, String)>) -> Array<SQLiteRow> {
        return rows.map { queryPlan($0, $1, $2, $3) }
    }

    private func queryPlan(_ id: Int, _ parent: Int, _ notused: Int,
                           _ detail: String) -> SQLiteRow {
        return ["id": .integer(Int64(id)), "parent": .integer(Int64(parent)),
                "notused": .integer(Int64(notused)), "detail": .text(detail)]
    }
}
