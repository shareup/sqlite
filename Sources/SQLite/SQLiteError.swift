import Foundation
import SQLite3

public enum SQLiteError: Error, Equatable {
    case databaseIsClosed
    case onInternalError(NSError)
    case onInvalidPath(String)
    case onOpenSharedDatabase(String, String)
    case onOpen(Int32, String)
    case onEnableWAL(Int32)
    case onInvalidSQLiteVersion
    case onUnsupportedSQLiteVersion(Int, Int, Int)
    case onClose(Int32)
    case onPrepareStatement(Int32, String)
    case onGetParameterIndex(String)
    case onBindParameter(Int32, Int32, SQLiteValue)
    case onStep(Int32, String)
    case onWrite(Array<SQLiteRow>)
    case onGetColumnType(Int32)
    case onBeginTransactionAfterDeallocating
    case onExecuteQueryAfterDeallocating
    case onGetColumnInTable(String)
    case onGetIndexInTable(String)
    case onGetSQL
    case onInvalidTableName(String)
    case onDecodingRow(String)
    case onInvalidDecodingType(String)
    case onInvalidSelectStatementColumnCount
    case onSubscribeWithoutColumnMetadata
    case onSubscribeWithoutDatabase
    case onTryToObserveZeroTables
}

extension SQLiteError {
    public var code: Int32? {
        switch self {
        case .databaseIsClosed:
            return nil
        case .onInternalError:
            return nil
        case .onInvalidPath:
            return nil
        case .onOpenSharedDatabase:
            return nil
        case let .onOpen(code, _):
            return code
        case let .onEnableWAL(code):
            return code
        case .onInvalidSQLiteVersion:
            return nil
        case .onUnsupportedSQLiteVersion:
            return nil
        case let .onClose(code):
            return code
        case let .onPrepareStatement(code, _):
            return code
        case .onGetParameterIndex:
            return nil
        case let .onBindParameter(code, _, _):
            return code
        case let .onStep(code, _):
            return code
        case .onWrite:
            return nil
        case .onGetColumnType:
            return nil
        case .onBeginTransactionAfterDeallocating:
            return nil
        case .onExecuteQueryAfterDeallocating:
            return nil
        case .onGetColumnInTable:
            return nil
        case .onGetIndexInTable:
            return nil
        case .onGetSQL:
            return nil
        case .onInvalidTableName:
            return nil
        case .onDecodingRow:
            return nil
        case .onInvalidDecodingType:
            return nil
        case .onInvalidSelectStatementColumnCount:
            return nil
        case .onSubscribeWithoutColumnMetadata:
            return nil
        case .onSubscribeWithoutDatabase:
            return nil
        case .onTryToObserveZeroTables:
            return nil
        }
    }

    public var isBusy: Bool {
        guard let code = code else { return false }
        return code == SQLITE_BUSY
    }

    public var isClosed: Bool {
        guard case .databaseIsClosed = self else { return false }
        return true
    }
}

extension SQLiteError: CustomStringConvertible {
    public var description: String {
        func string(for code: Int32) -> String {
            return String(cString: sqlite3_errstr(code))
        }

        switch self {
        case .databaseIsClosed:
            return "Database is closed"
        case .onInternalError(let error):
            return "Internal error: '\(String(describing: error))'"
        case .onInvalidPath(let path):
            return "Invalid path: '\(path)'"
        case .onOpenSharedDatabase(let path, let error):
            return "Could not open shared database at '\(path)': \(error)"
        case .onOpen(let code, let path):
            return "Could not open database at '\(path)': \(string(for: code))"
        case let .onEnableWAL(code):
            return "Could not enable WAL mode: \(string(for: code))"
        case .onInvalidSQLiteVersion:
            return "Invalid SQLite version"
        case let .onUnsupportedSQLiteVersion(major, minor, patch):
            return "Unsupported SQLite version: \(major).\(minor).\(patch)"
        case .onClose(let code):
            return "Could not close database: \(string(for: code))"
        case .onPrepareStatement(let code, let sql):
            return "Could not prepare statement for '\(sql)': \(string(for: code))"
        case .onGetParameterIndex(let parameterName):
            return "Could not get index for '\(parameterName)'"
        case .onBindParameter(let code, let index, let value):
            return "Could not bind \(value) to \(index): \(string(for: code))"
        case .onBeginTransactionAfterDeallocating:
            return "Tried to begin a transaction after deallocating"
        case .onExecuteQueryAfterDeallocating:
            return "Tried to execute a query after deallocating"
        case .onWrite(let result):
            return "Write returned results: '\(result)'"
        case .onGetColumnType(let type):
            return "Invalid column type: \(type)"
        case .onStep(let code, let sql):
            return "Could not execute SQL '\(sql)': \(string(for: code))"
        case .onGetColumnInTable(let error):
            return "Could not get column in table: \(error)"
        case .onGetIndexInTable(let error):
            return "Could not get index in table: \(error)"
        case .onGetSQL:
            return "Could not get SQL for prepared statement"
        case .onInvalidTableName(let tableName):
            return "'\(tableName)' is not a valid table name"
        case .onDecodingRow(let valueName):
            return "Could not decode value for '\(valueName)'"
        case .onInvalidDecodingType(let typeDescription):
            return "Could not decode value of type '\(typeDescription)'"
        case .onInvalidSelectStatementColumnCount:
            return "A SELECT statement must contain at least one result column"
        case .onSubscribeWithoutColumnMetadata:
            return "Could not subscribe to database because SQLite was not compiled with SQLITE_ENABLE_COLUMN_METADATA"
        case .onSubscribeWithoutDatabase:
            return "Could not subscribe because the SQLite database has been deallocated"
        case .onTryToObserveZeroTables:
            return "Could not observe database because no observable tables were found"
        }
    }
}
