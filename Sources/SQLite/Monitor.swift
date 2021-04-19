import Foundation
import SQLite3
import Synchronized

private class WeakObserver {
    weak var observer: Observer?
    var isNil: Bool { return self.observer == nil }

    init(observer: Observer) {
        self.observer = observer
    }
}

class Monitor: NSObject {
    private weak var _database: SQLiteDatabase?
    private let _observers = Observers()
    private var _updatedTables = Set<String>()

    private let _id: String = UUID().uuidString
    private let _coordinatorURL: URL?
    private lazy var _coordinatorQueue: OperationQueue = {
        let queue = OperationQueue()
        queue.name = "app.shareup.sqlite.monitor"
        queue.maxConcurrentOperationCount = 1
        return queue
    }()

    init(database: SQLiteDatabase) {
        _database = database
        _coordinatorURL =
            database.path == ":memory:" ?
            nil :
            URL(
                fileURLWithPath: database.path.appending("-monitor"),
                isDirectory: false
            )
        super.init()
        registerFilePresenter()
    }

    deinit {
        removeAllObservers()
    }

    func observe(
        statement: SQLiteStatement,
        queue: DispatchQueue = .main,
        block: @escaping (Array<SQLiteRow>) -> Void
    ) throws -> AnyObject {
        guard let database = _database else {
            throw SQLiteError.onInternalError("Database is missing")
        }

        let tables = try tablesToObserve(for: statement, in: database)
        guard tables.isEmpty == false else { throw SQLiteError.onTryToObserveZeroTables }

        if _observers.isEmpty {
            createUpdateCommitAndRollbackHandlers(in: database)
        }

        let observer = Observer(
            monitor: self,
            statement: statement,
            tables: tables,
            queue: queue,
            block: block
        )
        _observers.add(observer: observer)

        return observer
    }

    func remove(observer: AnyObject) {
        guard let anObserver = observer as? Observer else { return }
        _observers.remove(observer: anObserver)
        cleanUpObservers()
    }

    func removeAllObservers() {
        _observers.removeAll()
        cleanUpObservers()
        removeFilePresenter()
    }

    private func cleanUpObservers() {
        guard let database = _database else { return }

        _observers.compact()
        if _observers.isEmpty {
            removeUpdateCommitAndRollbackHandlers(in: database)
        }
    }
}

extension Monitor {
    private func createUpdateCommitAndRollbackHandlers(in database: SQLiteDatabase) {
        database.createUpdateHandler { [weak self] (table) in
            self?._updatedTables.insert(table)
        }

        database.createCommitHandler { [weak self] in
            guard let self = self else { return }
            guard let database = self._database else { return }
            database.notify(observers: self._observers.matching(tables: self._updatedTables))
            self._updatedTables.removeAll()
            self.notifyOtherProcesses()
        }

        database.createRollbackHandler { [weak self] in
            self?._updatedTables.removeAll()
        }
    }

    private func removeUpdateCommitAndRollbackHandlers(in database: SQLiteDatabase) {
        database.removeUpdateHandler()
        database.removeCommitHandler()
        database.removeRollbackHandler()
    }
}

extension Monitor: NSFilePresenter {
    var presentedItemURL: URL? { _coordinatorURL }
    var presentedItemOperationQueue: OperationQueue { _coordinatorQueue }

    func registerFilePresenter() {
        guard let url = _coordinatorURL else { return }
        touch(url)
        NSFileCoordinator.addFilePresenter(self)
    }

    func removeFilePresenter() {
        NSFileCoordinator.removeFilePresenter(self)
    }

    func presentedItemDidChange() {
        _database?.notify(observers: _observers.all)
    }

    func notifyOtherProcesses() {
        _coordinatorQueue.addOperation { [weak self] in
            guard let self = self else { return }
            guard let url = self._coordinatorURL else { return }

            let coordinator = NSFileCoordinator(filePresenter: self)
            coordinator.coordinate(
                writingItemAt: url,
                options: .forReplacing,
                error: nil,
                byAccessor: self.touch
            )
        }
    }

    private var touch: (URL?) -> Void {
        { [id = _id] (url) in
            guard let url = url else { return }
            try? id.write(to: url, atomically: true, encoding: .utf8)
        }
    }
}

extension Monitor {
    private func tablesToObserve(
        for statement: OpaquePointer,
        in database: SQLiteDatabase
    ) throws -> Set<String> {
        guard let sql = sqlite3_sql(statement) else { throw SQLiteError.onGetSQL }
        let explain = "EXPLAIN QUERY PLAN \(String(cString: sql));"
        let queryPlan = try database.execute(raw: explain)
        return QueryPlanParser.tables(in: queryPlan, matching: try database.tables())
    }
}

private class Observers {
    private var _observers = Array<WeakObserver>()
    private let _lock = Lock()

    var isEmpty: Bool { return _lock.locked { _observers.isEmpty } }

    var all: Array<Observer> {
        return _lock.locked { _observers.compactMap { $0.observer } }
    }

    func add(observer: Observer) {
        _lock.locked {
            unsafeRemove(observer: observer)
            _observers.append(WeakObserver(observer: observer))
        }
    }

    func matching(tables: Set<String>) -> Array<Observer> {
        return _lock.locked {
            _observers
                .compactMap { $0.observer }
                .filter { $0.tables.intersects(tables) }
        }
    }

    func remove(observer: Observer) {
        _lock.locked {
            unsafeRemove(observer: observer)
        }
    }

    func removeAll() {
        _lock.locked {
            _observers.forEach { $0.observer?.finalize() }
            _observers.removeAll()
        }
    }

    func compact() {
        _lock.locked {
            _observers = _observers.compactMap { $0.isNil ? nil : $0 }
        }
    }

    private func unsafeRemove(observer: Observer) {
        _observers = _observers.compactMap { (weakObserver) -> WeakObserver? in
            guard let anObserver = weakObserver.observer else { return nil }
            return observer == anObserver ? nil : weakObserver
        }
    }
}

private extension Set {
    func intersects(_ other: Set<Element>) -> Bool {
        return self.intersection(other).isEmpty == false
    }
}
