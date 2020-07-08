import Foundation

struct QueryPlanParser {
    static func tables(in queryPlan: Array<SQLiteRow>,
                       matching databaseTables: Array<String>) -> Set<String> {
        let databaseTables = databaseTables.sortedByLongestToShortest()
        var tables = Set<String>()
        for row in queryPlan {
            guard let detail = row["detail"]?.stringValue else { continue }
            guard let start = detail.tableNameStart else { continue }
            guard let end = detail.tableNameEnd(startingAt: start, matching: databaseTables) else { continue }
            let table = detail[start..<end]
            guard table.isEmpty == false else { continue }
            tables.insert(String(table))
        }
        return tables
    }
}

private extension String {
    var tableNameStart: String.Index? {
        guard hasPrefix("SCAN TABLE ") || hasPrefix("SEARCH TABLE ") else { return nil }
        return range(of: " TABLE ")?.upperBound
    }

    func tableNameEnd(startingAt start: String.Index, matching databaseTables: Array<String>) -> String.Index? {
        for table in databaseTables {
            if let end = range(of: table, options: [.anchored], range: start..<endIndex) {
                return end.upperBound
            }
        }
        let substring = String(self[start..<endIndex])
        return databaseTables.contains(substring) ? endIndex : nil
    }
}

private extension Array where Element == String {
    func sortedByLongestToShortest() -> Array<String> {
        return sorted(by: { $0.count > $1.count })
    }
}
