import Foundation
import GRDB
import os.log

struct SQLiteVersion: Comparable, CustomStringConvertible {
    let major: Int
    let minor: Int
    let patch: Int

    init(_ database: SQLiteDatabase) throws {
        try self.init(rows: database.execute(raw: Self.selectVersion))
    }

    init(major: Int, minor: Int, patch: Int) {
        self.major = major
        self.minor = minor
        self.patch = patch
    }

    init(row: Row) throws {
        guard let version = (row["version"] as? String)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        else {
            os_log(
                "version: error=missing version key",
                log: log,
                type: .error
            )
            throw SQLiteError.SQLITE_ERROR
        }

        let components = version.components(separatedBy: ".").compactMap(Int.init)
        guard components.count == 3 else {
            os_log(
                "version: error=incorrect number of components",
                log: log,
                type: .error
            )
            throw SQLiteError.SQLITE_ERROR
        }
        major = components[0]
        minor = components[1]
        patch = components[2]
    }

    init(rows: [SQLiteRow]) throws {
        guard let row = rows.first,
              let version = row["version"]?
              .stringValue?
              .trimmingCharacters(in: .whitespacesAndNewlines)
        else {
            os_log(
                "version: error=missing version key",
                log: log,
                type: .error
            )
            throw SQLiteError.SQLITE_ERROR
        }

        let components = version.components(separatedBy: ".").compactMap(Int.init)
        guard components.count == 3 else {
            os_log(
                "version: error=incorrect number of components",
                log: log,
                type: .error
            )
            throw SQLiteError.SQLITE_ERROR
        }
        major = components[0]
        minor = components[1]
        patch = components[2]
    }

    static var selectVersion: SQL { "SELECT sqlite_version() AS version;" }

    static func == (lhs: SQLiteVersion, rhs: SQLiteVersion) -> Bool {
        lhs.major == rhs.major && lhs.minor == rhs.minor && lhs.patch == rhs.patch
    }

    static func < (lhs: SQLiteVersion, rhs: SQLiteVersion) -> Bool {
        guard lhs.major <= rhs.major else { return false }
        guard lhs.major == rhs.major else {
            // 2.x.x < 3.x.x
            return true
        }

        guard lhs.minor <= rhs.minor else { return false }
        guard lhs.minor == rhs.minor else {
            // 3.2.x < 3.3.x
            return true
        }

        return lhs.patch < rhs.patch
    }

    var isSupported: Bool {
        self >= SQLiteVersion(major: 3, minor: 24, patch: 0) &&
            self < SQLiteVersion(major: 4, minor: 0, patch: 0)
    }

    var description: String { "\(major).\(minor).\(patch)" }
}
