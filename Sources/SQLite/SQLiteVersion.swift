import Foundation

struct SQLiteVersion: Comparable {
    let major: Int
    let minor: Int
    let patch: Int

    init(major: Int, minor: Int, patch: Int) {
        self.major = major
        self.minor = minor
        self.patch = patch
    }

    init(rows: [SQLiteRow]) throws {
        guard let row = rows.first,
              let version = row["version"]?
              .stringValue?
              .trimmingCharacters(in: .whitespacesAndNewlines)
        else { throw SQLiteError.onInvalidSQLiteVersion }

        let components = version.components(separatedBy: ".").compactMap(Int.init)
        guard components.count == 3 else { throw SQLiteError.onInvalidSQLiteVersion }
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
}
