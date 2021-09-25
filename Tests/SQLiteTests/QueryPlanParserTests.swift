import XCTest
import SQLite3
@testable import SQLite

class QueryPlanParserTests: XCTestCase {
    func testColumnsFromSingleTables_v3_24_0() throws {
        let tables = ["conversations", "TABLE", "SCAN"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([(1, 0, 0, "SCAN TABLE conversations")])
        let expected: Set<String> = ["conversations"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_24_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsFromSingleTables_v3_36_0() throws {
        let tables = ["conversations", "TABLE", "SCAN"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([(1, 0, 0, "SCAN conversations")])
        let expected: Set<String> = ["conversations"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_36_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsFromMultipleTables_v3_24_0() throws {
        let tables = ["✌🏼 table", "first table", "sqlite_autoindex_✌🏼 table_1", "USING"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (1, 0, 0, "SEARCH TABLE ✌🏼 table USING INDEX sqlite_autoindex_✌🏼 table_1 (id column=?)"),
            (4, 0, 0, "SEARCH TABLE first table USING INDEX sqlite_autoindex_first table_1 (id column=?)")
        ])
        let expected: Set<String> = ["first table", "✌🏼 table"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_24_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsFromMultipleTables_v3_36_0() throws {
        let tables = ["✌🏼 table", "first table", "sqlite_autoindex_✌🏼 table_1", "USING"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (1, 0, 0, "SEARCH ✌🏼 table USING INDEX sqlite_autoindex_✌🏼 table_1 (id column=?)"),
            (4, 0, 0, "SEARCH first table USING INDEX sqlite_autoindex_first table_1 (id column=?)")
        ])
        let expected: Set<String> = ["first table", "✌🏼 table"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_36_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsWithMergesJoinsAndJSON_v3_24_0() throws {
        let tables = ["AS", "text_messages", "providers", "patients", "|||"]
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
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_24_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsWithMergesJoinsAndJSON_v3_36_0() throws {
        let tables = ["AS", "text_messages", "providers", "patients", "|||"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (1, 0, 0, "MERGE (UNION ALL)"),
            (3, 1, 0, "LEFT"),
            (10, 3, 0, "SEARCH text_messages USING INDEX text_messages_index"),
            (18, 3, 0, "SCAN patients"),
            (27, 3, 0, "SCAN json_each AS USING VIRTUAL TABLE INDEX 1:"),
            (52, 1, 0, "RIGHT"),
            (62, 52, 0, "SEARCH text_messages USING CONVERING INDEX sqlite_autoindex_1"),
            (66, 52, 0, "SCAN json_each AS USING VIRTUAL TABLE INDEX 1:"),
        ])
        let expected: Set<String> = ["text_messages", "patients"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_36_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsWithSimilarNames_v3_24_0() throws {
        let tables = ["a", "ab", "abc", "abcd"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (1, 0, 0, "SCAN TABLE a"),
            (3, 1, 0, "SCAN TABLE abcd"),
            (10, 3, 0, "SEARCH TABLE ab USING INDEX ab_index"),
        ])
        let expected: Set<String> = ["a", "ab", "abcd"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_24_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsWithSimilarNames_v3_36_0() throws {
        let tables = ["a", "ab", "abc", "abcd"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (1, 0, 0, "SCAN a"),
            (3, 1, 0, "SCAN abcd"),
            (10, 3, 0, "SEARCH ab USING INDEX ab_index"),
        ])
        let expected: Set<String> = ["a", "ab", "abcd"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_36_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsWithReservedWordsAndControlCharacters_v3_24_0() throws {
        let tables = ["USING", "| |", "AS", "&&", "||", "USING AS"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (1, 0, 0, "SEARCH TABLE USING AS USING USING_AS_index"),
            (3, 1, 0, "SCAN TABLE &&"),
            (10, 3, 0, "SEARCH TABLE | | USING INDEX ab_index"),
        ])
        let expected: Set<String> = ["USING AS", "&&", "| |"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_24_0)
        XCTAssertEqual(expected, actual)
    }

    func testColumnsWithReservedWordsAndControlCharacters_v3_36_0() throws {
        let tables = ["USING", "| |", "AS", "&&", "||", "USING AS"]
        let queryPlan: Array<SQLiteRow> = self.queryPlan([
            (1, 0, 0, "SEARCH USING AS USING USING_AS_index"),
            (3, 1, 0, "SCAN &&"),
            (10, 3, 0, "SEARCH | | USING INDEX ab_index"),
        ])
        let expected: Set<String> = ["USING AS", "&&", "| |"]
        let actual = QueryPlanParser.tables(in: queryPlan, matching: tables, for: v3_36_0)
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

    private var v3_24_0: SQLiteVersion { .init(major: 3, minor: 24, patch: 0) }
    private var v3_36_0: SQLiteVersion { .init(major: 3, minor: 36, patch: 0) }
}
