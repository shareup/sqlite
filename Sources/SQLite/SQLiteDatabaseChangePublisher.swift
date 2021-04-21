import Foundation
import Combine
import Synchronized

enum SQLiteDatabaseChange {
    case open
    case close
    case updateTables(Set<String>)
    case crossProcessUpdate
}

final class SQLiteDatabaseChangePublisher: NSObject, Publisher {
    typealias Output = SQLiteDatabaseChange
    typealias Failure = Never

    private let id = UUID().uuidString
    private weak var database: SQLiteDatabase?
    private let changeTrackerURL: URL?
    private let subject = PassthroughSubject<Output, Never>()

    private var updatedTables = Set<String>()
    private let queue: OperationQueue

    init(database: SQLiteDatabase) {
        self.database = database
        changeTrackerURL = database.changeTrackerURL

        queue = OperationQueue()
        queue.name = "app.shareup.sqlite.sqlitedatabasechangepublisher"
        queue.maxConcurrentOperationCount = 1

        super.init()

        createHooks()
        registerFilePresenter()
    }

    deinit {
        removeHooks()
    }

    func open() {
        subject.send(.open)
        createHooks()
        registerFilePresenter()
    }

    func close() {
        removeHooks()
        removeFilePresenter()
        subject.send(.close)
    }

    func receive<S: Subscriber>(
        subscriber: S
    ) where Failure == S.Failure, Output == S.Input {
        subject.receive(subscriber: subscriber)
    }
}

private extension SQLiteDatabaseChangePublisher {
    func createHooks() {
        guard let database = database else { return }

        database.createUpdateHandler { [weak self] (table) in
            guard let self = self else { return }
            self.updatedTables.insert(table)
        }

        database.createCommitHandler { [weak self] in
            guard let self = self else { return }
            let tables = self.updatedTables
            self.updatedTables.removeAll()
            self.notifySubject(tables: tables)
            self.notifyOtherProcesses()
        }

        database.createRollbackHandler { [weak self] in
            guard let self = self else { return }
            self.updatedTables.removeAll()
        }
    }

    func removeHooks() {
        guard let database = database else { return }
        database.removeUpdateHandler()
        database.removeCommitHandler()
        database.removeRollbackHandler()
    }

    private func notifySubject(tables: Set<String>) {
        let operation = BlockOperation { [weak self] in
            self?.subject.send(.updateTables(tables))
        }
        operation.queuePriority = .veryHigh
        queue.addOperation(operation)
    }
}

extension SQLiteDatabaseChangePublisher: NSFilePresenter {
    var presentedItemURL: URL? { changeTrackerURL }
    var presentedItemOperationQueue: OperationQueue { queue }

    func presentedItemDidChange() {
        subject.send(.crossProcessUpdate)
    }

    private func registerFilePresenter() {
        guard let url = changeTrackerURL else { return }
        touch(url)
        NSFileCoordinator.addFilePresenter(self)
    }

    private func removeFilePresenter() {
        NSFileCoordinator.removeFilePresenter(self)
    }

    private func notifyOtherProcesses() {
        let operation = BlockOperation { [weak self] in
            guard let self = self else { return }
            guard let url = self.changeTrackerURL else { return }

            let coordinator = NSFileCoordinator(filePresenter: self)
            coordinator.coordinate(
                writingItemAt: url,
                options: .forReplacing,
                error: nil,
                byAccessor: self.touch
            )
        }
        operation.queuePriority = .low
        queue.addOperation(operation)
    }

    private var touch: (URL?) -> Void {
        { [id] (url) in
            guard let url = url else { return }
            try? id.write(to: url, atomically: true, encoding: .utf8)
        }
    }
}

private extension SQLiteDatabase {
    var changeTrackerURL: URL? {
        guard path != ":memory:" else { return nil }
        return URL(fileURLWithPath: path.appending("-change-tracker"), isDirectory: false)
    }
}
