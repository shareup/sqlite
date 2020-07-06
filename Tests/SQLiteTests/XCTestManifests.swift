import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(CodableTests.allTests),
        testCase(DatabaseTests.allTests),
        testCase(PreciseDateFormatterTests.allTests),
        testCase(ObserveTests.allTests),
        testCase(PublisherTests.allTests),
        testCase(QueryPlanParserTests.allTests),
        testCase(SQLiteRowExtensionsTests.allTests),
    ]
}
#endif
