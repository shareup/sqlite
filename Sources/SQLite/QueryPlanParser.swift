import Foundation

struct QueryPlanParser {
    static func tables(
        in queryPlan: Array<SQLiteRow>,
        matching databaseTables: Array<String>,
        for sqliteVersion: SQLiteVersion
    ) -> Set<String> {
        let databaseTables = databaseTables.sortedByLongestToShortest()
        var tables = Set<String>()
        for row in queryPlan {
            guard let detail = row["detail"]?.stringValue else { continue }
            guard let start = detail.tableNameStart(for: sqliteVersion) else { continue }
            guard let end = detail.tableNameEnd(startingAt: start, matching: databaseTables) else { continue }
            let table = detail[start..<end]
            guard table.isEmpty == false else { continue }
            tables.insert(String(table))
        }
        return tables
    }
}

private extension String {
    func tableNameStart(for version: SQLiteVersion) -> String.Index? {
        if version >= v3_24_0 && version < v3_36_0 {
            guard hasPrefix("SCAN TABLE ") || hasPrefix("SEARCH TABLE ") else { return nil }
            return range(of: " TABLE ")?.upperBound
        } else if version >= v3_36_0 {
            if hasPrefix("SCAN ") {
                return range(of: "SCAN ")?.upperBound
            } else if hasPrefix("SEARCH ") {
                return range(of: "SEARCH ")?.upperBound
            } else {
                return nil
            }
        } else {
            assertionFailure()
            return nil
        }
    }

    func tableNameEnd(
        startingAt start: String.Index,
        matching databaseTables: Array<String>
    ) -> String.Index? {
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

private let v3_24_0 = SQLiteVersion(major: 3, minor: 24, patch: 0)
private let v3_36_0 = SQLiteVersion(major: 3, minor: 36, patch: 0)
