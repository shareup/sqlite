import Foundation
import SQLite3

private class WeakObserver {
    weak var observer: SQLite.Observer?
    var isNil: Bool { return self.observer == nil }

    init(observer: SQLite.Observer) {
        self.observer = observer
    }
}

extension SQLite {
    class Monitor {
        private weak var _database: SQLite.Database?
        private let _observers = Observers()
        private var _updatedTables = Set<String>()

        private var _notificationQueue = DispatchQueue(label: "SQLite.Monitor Notification Queue")

        init(database: SQLite.Database) {
            _database = database
        }

        deinit {
            removeAllObservers()
        }

        func observe(statement: Statement, queue: DispatchQueue = .main,
                     block: @escaping (Array<SQLiteRow>) -> Void) throws -> AnyObject {
            guard let database = _database else {
                throw SQLite.Error.onInternalError("SQLite.Database is missing")
            }

            let tables = try tablesToObserve(for: statement, in: database)
            assert(tables.isEmpty == false)

            if _observers.isEmpty {
                createUpdateCommitAndRollbackHandlers(in: database)
            }

            let observer = Observer(monitor: self, statement: statement, tables: tables, queue: queue, block: block)
            _observers.add(observer: observer)

            return observer
        }

        func remove(observer: AnyObject) {
            guard let anObserver = observer as? SQLite.Observer else { return }
            _observers.remove(observer: anObserver)
            cleanUpObservers()
        }

        func removeAllObservers() {
            _observers.removeAll()
            cleanUpObservers()
        }

        private func cleanUpObservers() {
            guard let database = _database else { return }

            _observers.compact()
            if _observers.isEmpty {
                removeUpdateCommitAndRollbackHandlers(in: database)
            }
        }
    }
}

extension SQLite.Monitor {
    private func createUpdateCommitAndRollbackHandlers(in database: SQLite.Database) {
        database.createUpdateHandler { [weak self] (table) in
            self?._updatedTables.insert(table)
        }

        database.createCommitHandler { [weak self] in
            guard let self = self else { return }
            guard let database = self._database else { return }
            database.notify(observers: self._observers.matching(tables: self._updatedTables))
            self._updatedTables.removeAll()
        }

        database.createRollbackHandler { [weak self] in
            self?._updatedTables.removeAll()
        }
    }

    private func removeUpdateCommitAndRollbackHandlers(in database: SQLite.Database) {
        database.removeUpdateHandler()
        database.removeCommitHandler()
        database.removeRollbackHandler()
    }
}

extension SQLite.Monitor {
    private func tablesToObserve(for statement: OpaquePointer,
                                in database: SQLite.Database) throws -> Set<String> {
        guard let sql = sqlite3_sql(statement) else { throw SQLite.Error.onGetSQL }
        let explain = "EXPLAIN QUERY PLAN \(String(cString: sql));"
        let queryPlan = try database.execute(raw: explain)
        return SQLite.QueryPlanParser.tables(in: queryPlan, matching: try database.tables())
    }
}

private class Observers {
    private var _observers = Array<WeakObserver>()

    var isEmpty: Bool { return _observers.isEmpty }

    func add(observer: SQLite.Observer) {
        remove(observer: observer)
        _observers.append(WeakObserver(observer: observer))
    }

    func matching(tables: Set<String>) -> Array<SQLite.Observer> {
        return _observers
            .compactMap { $0.observer }
            .filter { $0.tables.intersects(tables) }
    }

    func remove(observer: SQLite.Observer) {
        _observers = _observers.compactMap { (weakObserver) -> WeakObserver? in
            guard let anObserver = weakObserver.observer else { return nil }
            return observer == anObserver ? nil : weakObserver
        }
    }

    func removeAll() {
        _observers.removeAll()
    }

    func compact() {
        _observers = _observers.compactMap { $0.isNil ? nil : $0 }
    }
}

private extension Set {
    func intersects(_ other: Set<Element>) -> Bool {
        return self.intersection(other).isEmpty == false
    }
}
