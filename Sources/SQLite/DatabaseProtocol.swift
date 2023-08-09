import Foundation

public protocol DatabaseProtocol {
    func inTransaction<T>(
        _ block: (DatabaseProtocol) throws -> T
    ) throws -> T

    @_disfavoredOverload
    func read(
        _ sql: SQL,
        arguments: SQLiteArguments
    ) throws -> [SQLiteRow]

    func read<T: SQLiteTransformable>(
        _ sql: SQL,
        arguments: SQLiteArguments
    ) throws -> [T]

    func write(_ sql: SQL, arguments: SQLiteArguments) throws

    @discardableResult
    func execute(raw: SQL) throws -> [SQLiteRow]
}

public extension DatabaseProtocol {
    @_disfavoredOverload
    func read(_ sql: SQL) throws -> [SQLiteRow] {
        try read(sql, arguments: [:])
    }

    func read<T: SQLiteTransformable>(_ sql: SQL) throws -> [T] {
        try read(sql, arguments: [:])
    }

    func write(_ sql: SQL) throws {
        try write(sql, arguments: [:])
    }
}
