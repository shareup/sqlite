import Foundation
import SQLite3

extension SQLite {
    public enum Error: Swift.Error {
        case onOpen(Int32, String)
        case onClose(Int32)
        case onPrepareStatement(Int32, String)
        case onGetParameterIndex(String)
        case onBindParameter(Int32, Int32, SQLite.Value)
        case onStep(Int32, String)
        case onGetColumnType(Int32)
    }
}

extension SQLite.Error: CustomStringConvertible {
    public var description: String {
        func string(for code: Int32) -> String {
            return String(cString: sqlite3_errstr(code))
        }

        switch self {
        case .onOpen(let code, let path):
            return "Could not open database at '\(path)': \(string(for: code))"
        case .onClose(let code):
            return "Could not close database: \(string(for: code))"
        case .onPrepareStatement(let code, let sql):
            return "Could not prepare statement for '\(sql)': \(string(for: code))"
        case .onGetParameterIndex(let parameterName):
            return "Could not get index for '\(parameterName)'"
        case .onBindParameter(let code, let index, let value):
            return "Could not bind \(value) to \(index): \(string(for: code))"
        case .onStep(let code, let sql):
            return "Could not execute SQL '\(sql)': \(string(for: code))"
        case .onGetColumnType(let type):
            return "Invalid column type: \(type)"
        }
    }
}
