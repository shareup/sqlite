import Foundation
import SQLite3

class Observer: Hashable {
    weak var monitor: Monitor?
    let statement: Statement
    let tables: Set<String>
    let queue: DispatchQueue
    let block: (Array<SQLiteRow>) -> Void

    init(monitor: Monitor, statement: Statement, tables: Set<String>,
         queue: DispatchQueue, block: @escaping (Array<SQLiteRow>) -> Void) {
        self.monitor = monitor
        self.statement = statement
        self.tables = tables
        self.queue = queue
        self.block = block
    }

    deinit {
        sqlite3_finalize(statement)
        monitor?.remove(observer: self)
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self))
    }

    static func == (lhs: Observer, rhs: Observer) -> Bool {
        return ObjectIdentifier(lhs) == ObjectIdentifier(rhs)
    }
}
