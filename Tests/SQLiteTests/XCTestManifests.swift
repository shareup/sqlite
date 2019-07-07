import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(SQLiteCodableTests.allTests),
        testCase(SQLiteDatabaseTests.allTests),
        testCase(SQLiteDateFormatterTests.allTests),
        testCase(SQLiteObserveTests.allTests),
        testCase(SQLitePublisherTests.allTests),
        testCase(QueryPlanParserTests.allTests),
        testCase(SQLiteRowExtensionsTests.allTests),
    ]
}
#endif
