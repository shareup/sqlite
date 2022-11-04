import Foundation
import SQLite3

typealias UpdateHookCallback =
    (UnsafeMutableRawPointer?, Int32, UnsafePointer<Int8>?, UnsafePointer<Int8>?, Int64) -> Void

class Hook {
    var update: UpdateHookCallback?
    var commit: (() -> Void)?
    var rollback: (() -> Void)?
}

func updateHookWrapper(
    context: UnsafeMutableRawPointer?,
    operationType: Int32,
    databaseName: UnsafePointer<Int8>?,
    tableName: UnsafePointer<Int8>?,
    rowid: sqlite3_int64
) {
    guard let context else { return }
    let hook = Unmanaged<Hook>.fromOpaque(context).takeUnretainedValue()
    hook.update?(context, operationType, databaseName, tableName, rowid)
}

func commitHookWrapper(context: UnsafeMutableRawPointer?) -> Int32 {
    guard let context else { return 0 }
    let hook = Unmanaged<Hook>.fromOpaque(context).takeUnretainedValue()
    hook.commit?()
    return 0
}

func rollbackHookWrapper(context: UnsafeMutableRawPointer?) {
    guard let context else { return }
    let hook = Unmanaged<Hook>.fromOpaque(context).takeUnretainedValue()
    hook.rollback?()
}
