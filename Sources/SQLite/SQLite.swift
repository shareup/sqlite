import Foundation
import SQLite3

public typealias SQL = String
public typealias SQLiteStatement = OpaquePointer

let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
