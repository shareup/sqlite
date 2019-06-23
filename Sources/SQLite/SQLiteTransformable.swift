import Foundation

public protocol SQLiteTransformable {
    init(row: SQLiteRow) throws
}
