import XCTest
import SQLiteTests

var tests = [XCTestCaseEntry]()
tests += SQLiteTests.SQLiteCodableTests
tests += SQLiteTests.SQLiteDatabaseTests
tests += SQLiteTests.SQLiteDateFormatterTests
tests += SQLiteTests.SQLiteObserveTests
tests += SQLiteTests.SQLitePublisherTests
tests += SQLiteTests.QueryPlanParserTests
tests += SQLiteTests.SQLiteRowExtensionsTests
XCTMain(tests)
