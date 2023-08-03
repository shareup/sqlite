import Combine
import Foundation
import GRDB
import os.log
import SQLite3

public struct SQLiteError: Error, RawRepresentable, Hashable, Sendable {
    public let rawValue: Int32

    public init(rawValue: Int32) {
        self.rawValue = rawValue
    }
}

// NOTE: This is adapted directly from GRDB's DatabaseError

public extension SQLiteError {
    // swiftformat:disable all
    static let SQLITE_OK            = SQLiteError(rawValue: 0) // Successful result
    static let SQLITE_ERROR         = SQLiteError(rawValue: 1) // SQL error or missing database
    static let SQLITE_INTERNAL      = SQLiteError(rawValue: 2) // Internal logic error in SQLite
    static let SQLITE_PERM          = SQLiteError(rawValue: 3) // Access permission denied
    static let SQLITE_ABORT         = SQLiteError(rawValue: 4) // Callback routine requested an abort
    static let SQLITE_BUSY          = SQLiteError(rawValue: 5) // The database file is locked
    static let SQLITE_LOCKED        = SQLiteError(rawValue: 6) // A table in the database is locked
    static let SQLITE_NOMEM         = SQLiteError(rawValue: 7) // A malloc() failed
    static let SQLITE_READONLY      = SQLiteError(rawValue: 8) // Attempt to write a readonly database
    static let SQLITE_INTERRUPT     = SQLiteError(rawValue: 9) // Operation terminated by sqlite3_interrupt()
    static let SQLITE_IOERR         = SQLiteError(rawValue: 10) // Some kind of disk I/O error occurred
    static let SQLITE_CORRUPT       = SQLiteError(rawValue: 11) // The database disk image is malformed
    static let SQLITE_NOTFOUND      = SQLiteError(rawValue: 12) // Unknown opcode in sqlite3_file_control()
    static let SQLITE_FULL          = SQLiteError(rawValue: 13) // Insertion failed because database is full
    static let SQLITE_CANTOPEN      = SQLiteError(rawValue: 14) // Unable to open the database file
    static let SQLITE_PROTOCOL      = SQLiteError(rawValue: 15) // Database lock protocol error
    static let SQLITE_EMPTY         = SQLiteError(rawValue: 16) // Database is empty
    static let SQLITE_SCHEMA        = SQLiteError(rawValue: 17) // The database schema changed
    static let SQLITE_TOOBIG        = SQLiteError(rawValue: 18) // String or BLOB exceeds size limit
    static let SQLITE_CONSTRAINT    = SQLiteError(rawValue: 19) // Abort due to constraint violation
    static let SQLITE_MISMATCH      = SQLiteError(rawValue: 20) // Data type mismatch
    static let SQLITE_MISUSE        = SQLiteError(rawValue: 21) // Library used incorrectly
    static let SQLITE_NOLFS         = SQLiteError(rawValue: 22) // Uses OS features not supported on host
    static let SQLITE_AUTH          = SQLiteError(rawValue: 23) // Authorization denied
    static let SQLITE_FORMAT        = SQLiteError(rawValue: 24) // Auxiliary database format error
    static let SQLITE_RANGE         = SQLiteError(rawValue: 25) // 2nd parameter to sqlite3_bind out of range
    static let SQLITE_NOTADB        = SQLiteError(rawValue: 26) // File opened that is not a database file
    static let SQLITE_NOTICE        = SQLiteError(rawValue: 27) // Notifications from sqlite3_log()
    static let SQLITE_WARNING       = SQLiteError(rawValue: 28) // Warnings from sqlite3_log()
    static let SQLITE_ROW           = SQLiteError(rawValue: 100) // sqlite3_step() has another row ready
    static let SQLITE_DONE          = SQLiteError(rawValue: 101) // sqlite3_step() has finished executing

    // Extended Result Code
    // https://www.sqlite.org/rescode.html#extended_result_code_list

    static let SQLITE_ERROR_MISSING_COLLSEQ     = SQLiteError(rawValue: SQLITE_ERROR.rawValue | (1 << 8))
    static let SQLITE_ERROR_RETRY               = SQLiteError(rawValue: SQLITE_ERROR.rawValue | (2 << 8))
    static let SQLITE_ERROR_SNAPSHOT            = SQLiteError(rawValue: SQLITE_ERROR.rawValue | (3 << 8))
    static let SQLITE_IOERR_READ                = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (1 << 8))
    static let SQLITE_IOERR_SHORT_READ          = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (2 << 8))
    static let SQLITE_IOERR_WRITE               = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (3 << 8))
    static let SQLITE_IOERR_FSYNC               = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (4 << 8))
    static let SQLITE_IOERR_DIR_FSYNC           = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (5 << 8))
    static let SQLITE_IOERR_TRUNCATE            = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (6 << 8))
    static let SQLITE_IOERR_FSTAT               = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (7 << 8))
    static let SQLITE_IOERR_UNLOCK              = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (8 << 8))
    static let SQLITE_IOERR_RDLOCK              = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (9 << 8))
    static let SQLITE_IOERR_DELETE              = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (10 << 8))
    static let SQLITE_IOERR_BLOCKED             = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (11 << 8))
    static let SQLITE_IOERR_NOMEM               = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (12 << 8))
    static let SQLITE_IOERR_ACCESS              = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (13 << 8))
    static let SQLITE_IOERR_CHECKRESERVEDLOCK   = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (14 << 8))
    static let SQLITE_IOERR_LOCK                = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (15 << 8))
    static let SQLITE_IOERR_CLOSE               = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (16 << 8))
    static let SQLITE_IOERR_DIR_CLOSE           = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (17 << 8))
    static let SQLITE_IOERR_SHMOPEN             = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (18 << 8))
    static let SQLITE_IOERR_SHMSIZE             = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (19 << 8))
    static let SQLITE_IOERR_SHMLOCK             = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (20 << 8))
    static let SQLITE_IOERR_SHMMAP              = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (21 << 8))
    static let SQLITE_IOERR_SEEK                = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (22 << 8))
    static let SQLITE_IOERR_DELETE_NOENT        = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (23 << 8))
    static let SQLITE_IOERR_MMAP                = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (24 << 8))
    static let SQLITE_IOERR_GETTEMPPATH         = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (25 << 8))
    static let SQLITE_IOERR_CONVPATH            = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (26 << 8))
    static let SQLITE_IOERR_VNODE               = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (27 << 8))
    static let SQLITE_IOERR_AUTH                = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (28 << 8))
    static let SQLITE_IOERR_BEGIN_ATOMIC        = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (29 << 8))
    static let SQLITE_IOERR_COMMIT_ATOMIC       = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (30 << 8))
    static let SQLITE_IOERR_ROLLBACK_ATOMIC     = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (31 << 8))
    static let SQLITE_IOERR_DATA                = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (32 << 8))
    static let SQLITE_IOERR_CORRUPTFS           = SQLiteError(rawValue: SQLITE_IOERR.rawValue | (33 << 8))
    static let SQLITE_LOCKED_SHAREDCACHE        = SQLiteError(rawValue: SQLITE_LOCKED.rawValue | (1 << 8))
    static let SQLITE_LOCKED_VTAB               = SQLiteError(rawValue: SQLITE_LOCKED.rawValue | (2 << 8))
    static let SQLITE_BUSY_RECOVERY             = SQLiteError(rawValue: SQLITE_BUSY.rawValue | (1 << 8))
    static let SQLITE_BUSY_SNAPSHOT             = SQLiteError(rawValue: SQLITE_BUSY.rawValue | (2 << 8))
    static let SQLITE_BUSY_TIMEOUT              = SQLiteError(rawValue: SQLITE_BUSY.rawValue | (3 << 8))
    static let SQLITE_CANTOPEN_NOTEMPDIR        = SQLiteError(rawValue: SQLITE_CANTOPEN.rawValue | (1 << 8))
    static let SQLITE_CANTOPEN_ISDIR            = SQLiteError(rawValue: SQLITE_CANTOPEN.rawValue | (2 << 8))
    static let SQLITE_CANTOPEN_FULLPATH         = SQLiteError(rawValue: SQLITE_CANTOPEN.rawValue | (3 << 8))
    static let SQLITE_CANTOPEN_CONVPATH         = SQLiteError(rawValue: SQLITE_CANTOPEN.rawValue | (4 << 8))
    static let SQLITE_CANTOPEN_DIRTYWAL         = SQLiteError(rawValue: SQLITE_CANTOPEN.rawValue | (5 << 8)) /* Not Used */
    static let SQLITE_CANTOPEN_SYMLINK          = SQLiteError(rawValue: SQLITE_CANTOPEN.rawValue | (6 << 8))
    static let SQLITE_CORRUPT_VTAB              = SQLiteError(rawValue: SQLITE_CORRUPT.rawValue | (1 << 8))
    static let SQLITE_CORRUPT_SEQUENCE          = SQLiteError(rawValue: SQLITE_CORRUPT.rawValue | (2 << 8))
    static let SQLITE_CORRUPT_INDEX             = SQLiteError(rawValue: SQLITE_CORRUPT.rawValue | (3 << 8))
    static let SQLITE_READONLY_RECOVERY         = SQLiteError(rawValue: SQLITE_READONLY.rawValue | (1 << 8))
    static let SQLITE_READONLY_CANTLOCK         = SQLiteError(rawValue: SQLITE_READONLY.rawValue | (2 << 8))
    static let SQLITE_READONLY_ROLLBACK         = SQLiteError(rawValue: SQLITE_READONLY.rawValue | (3 << 8))
    static let SQLITE_READONLY_DBMOVED          = SQLiteError(rawValue: SQLITE_READONLY.rawValue | (4 << 8))
    static let SQLITE_READONLY_CANTINIT         = SQLiteError(rawValue: SQLITE_READONLY.rawValue | (5 << 8))
    static let SQLITE_READONLY_DIRECTORY        = SQLiteError(rawValue: SQLITE_READONLY.rawValue | (6 << 8))
    static let SQLITE_ABORT_ROLLBACK            = SQLiteError(rawValue: SQLITE_ABORT.rawValue | (2 << 8))
    static let SQLITE_CONSTRAINT_CHECK          = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (1 << 8))
    static let SQLITE_CONSTRAINT_COMMITHOOK     = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (2 << 8))
    static let SQLITE_CONSTRAINT_FOREIGNKEY     = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (3 << 8))
    static let SQLITE_CONSTRAINT_FUNCTION       = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (4 << 8))
    static let SQLITE_CONSTRAINT_NOTNULL        = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (5 << 8))
    static let SQLITE_CONSTRAINT_PRIMARYKEY     = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (6 << 8))
    static let SQLITE_CONSTRAINT_TRIGGER        = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (7 << 8))
    static let SQLITE_CONSTRAINT_UNIQUE         = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (8 << 8))
    static let SQLITE_CONSTRAINT_VTAB           = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (9 << 8))
    static let SQLITE_CONSTRAINT_ROWID          = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (10 << 8))
    static let SQLITE_CONSTRAINT_PINNED         = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (11 << 8))
    static let SQLITE_CONSTRAINT_DATATYPE       = SQLiteError(rawValue: SQLITE_CONSTRAINT.rawValue | (12 << 8))
    static let SQLITE_NOTICE_RECOVER_WAL        = SQLiteError(rawValue: SQLITE_NOTICE.rawValue | (1 << 8))
    static let SQLITE_NOTICE_RECOVER_ROLLBACK   = SQLiteError(rawValue: SQLITE_NOTICE.rawValue | (2 << 8))
    static let SQLITE_WARNING_AUTOINDEX         = SQLiteError(rawValue: SQLITE_WARNING.rawValue | (1 << 8))
    static let SQLITE_AUTH_USER                 = SQLiteError(rawValue: SQLITE_AUTH.rawValue | (1 << 8))
    static let SQLITE_OK_LOAD_PERMANENTLY       = SQLiteError(rawValue: SQLITE_OK.rawValue | (1 << 8))
    static let SQLITE_OK_SYMLINK                = SQLiteError(rawValue: SQLITE_OK.rawValue | (2 << 8))
    // swiftformat:enable all
}

extension SQLiteError: CustomStringConvertible {
    public var description: String {
        String(cString: sqlite3_errstr(rawValue))
    }
}

public extension SQLiteError {
    var isBusy: Bool { self == .SQLITE_BUSY }
    var isDatabaseClosed: Bool { self == .SQLITE_MISUSE }

    var isInterrupt: Bool {
        switch self {
        case SQLiteError.SQLITE_ABORT, SQLiteError.SQLITE_INTERRUPT:
            return true
        default:
            return false
        }
    }
}

public extension SQLiteError {
    /// Returns true if the pattern on the left matches the
    /// error on the right. Primary error codes match themselves
    /// and their extended error codes.
    ///
    /// - description: https://www.sqlite.org/rescode.html
    static func ~= (lhs: SQLiteError, rhs: Error) -> Bool {
        guard let err = rhs as? SQLiteError else { return false }
        if lhs == err {
            return true
        } else if lhs == SQLiteError(rawValue: err.rawValue & 0xFF) {
            return true
        } else {
            return false
        }
    }
}

extension SQLiteError {
    init(_ error: DatabaseError) {
        self = .init(rawValue: error.extendedResultCode.rawValue)
    }
}

func rethrowAsSQLiteError(_ error: Error) throws -> Never {
    if let dbError = error as? DatabaseError {
        throw SQLiteError(
            rawValue: dbError.extendedResultCode.rawValue
        )
    } else {
        throw error
    }
}

extension Publisher where Failure: Error {
    func mapToSQLiteError(sql: SQL? = nil) -> Publishers.MapError<Self, Error> {
        mapError { error in
            if let sql {
                os_log(
                    "publisher: sql=%s type=%{public}s error=%s",
                    log: log,
                    type: .error,
                    sql,
                    String(describing: type(of: self)),
                    String(describing: error)
                )
            } else {
                os_log(
                    "publisher: type=%{public}s error=%s",
                    log: log,
                    type: .error,
                    String(describing: type(of: self)),
                    String(describing: error)
                )
            }

            if let dbError = error as? DatabaseError {
                return SQLiteError(
                    rawValue: dbError.extendedResultCode.rawValue
                )
            } else {
                return error
            }
        }
    }
}
