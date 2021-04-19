import Foundation
import SQLite3

class Observer: Hashable {
    weak var monitor: Monitor?
    private(set) var statement: SQLiteStatement?
    let tables: Set<String>
    let queue: DispatchQueue
    let block: (Array<SQLiteRow>) -> Void

    init(
        monitor: Monitor,
        statement: SQLiteStatement,
        tables: Set<String>,
        queue: DispatchQueue,
        block: @escaping (Array<SQLiteRow>) -> Void
    ) {
        self.monitor = monitor
        self.statement = statement
        self.tables = tables
        self.queue = queue
        self.block = block
    }

    func finalize() {
        sqlite3_finalize(statement)
        statement = nil
    }

    deinit {
        finalize()
        monitor?.remove(observer: self)
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self))
    }

    static func == (lhs: Observer, rhs: Observer) -> Bool {
        return ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
    }
}
