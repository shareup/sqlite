import Combine
import Foundation
import Synchronized

enum SQLiteDatabaseChange {
    case open
    case close
    case updateTables(Set<String>)
    case updateAllTables
    case crossProcessUpdate
}

final class SQLiteDatabaseChangePublisher: NSObject, Publisher {
    typealias Output = SQLiteDatabaseChange
    typealias Failure = Never

    private let id = UUID().uuidString
    private weak var database: SQLiteDatabase?

    private let downstreamSubject = PassthroughSubject<Output, Never>()

    private let changeTrackerURL: URL?
    private let changeTrackerSubject = PassthroughSubject<Void, Never>()
    private var changeTrackerSubscription: AnyCancellable?

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
        subscribeToChangeTracker()
    }

    deinit {
        changeTrackerSubscription = nil
        removeHooks()
    }

    func open() {
        precondition(SQLiteQueue.isCurrentQueue)
        downstreamSubject.send(.open)
        createHooks()
        registerFilePresenter()
    }

    func close() {
        precondition(SQLiteQueue.isCurrentQueue)
        removeHooks()
        removeFilePresenter()
        downstreamSubject.send(.close)
    }

    func receive<S: Subscriber>(
        subscriber: S
    ) where Failure == S.Failure, Output == S.Input {
        downstreamSubject.receive(subscriber: subscriber)
    }
}

private extension SQLiteDatabaseChangePublisher {
    func createHooks() {
        guard let database else { return }

        SQLiteQueue.sync {
            database.createUpdateHandler { [weak self] table in
                guard let self else { return }
                self.updatedTables.insert(table)
            }

            database.createCommitHandler { [weak self] in
                guard let self else { return }
                let tables = self.updatedTables
                self.updatedTables.removeAll()
                // Some SQL statements do not trigger the update handler,
                // which means their affected tables aren't saved. An
                // example is `DELETE FROM <table>;`. In those cases,
                // assume every table has been updated.
                self.notifyDownstreamSubscribersAsync(
                    tables.isEmpty ? .updateAllTables : .updateTables(tables)
                )
                self.notifyOtherProcesses()
            }

            database.createRollbackHandler { [weak self] in
                guard let self else { return }
                self.updatedTables.removeAll()
            }
        }
    }

    func removeHooks() {
        guard let database else { return }

        SQLiteQueue.sync {
            database.removeUpdateHandler()
            database.removeCommitHandler()
            database.removeRollbackHandler()
        }
    }
}

extension SQLiteDatabaseChangePublisher: NSFilePresenter {
    var presentedItemURL: URL? { changeTrackerURL }
    var presentedItemOperationQueue: OperationQueue { queue }

    func presentedItemDidChange() {
        notifyDownstreamSubscribersAsync(.crossProcessUpdate)
    }

    private func registerFilePresenter() {
        guard let url = changeTrackerURL else { return }
        touch(url)
        NSFileCoordinator.addFilePresenter(self)
    }

    private func removeFilePresenter() {
        NSFileCoordinator.removeFilePresenter(self)
    }

    private func subscribeToChangeTracker() {
        let sub = changeTrackerSubject
            .throttle(
                for: .seconds(1),
                scheduler: RunLoop.main,
                // using `queue` breaks tests because it doesn't have a `RunLoop`
                latest: true
            )
            .receive(on: queue)
            .sink { [weak self] in
                guard let self else { return }
                guard let url = self.changeTrackerURL else { return }

                let coordinator = NSFileCoordinator(filePresenter: self)
                coordinator.coordinate(
                    writingItemAt: url,
                    options: .forReplacing,
                    error: nil,
                    byAccessor: self.touch
                )
            }
        changeTrackerSubscription = sub
    }

    private func notifyOtherProcesses() {
        changeTrackerSubject.send(())
    }

    private func notifyDownstreamSubscribersAsync(_ value: Output) {
        // When this callback is called, we could still be in the middle of a
        // database transaction because the commit handler gets called in
        // the middle of one. So, we can't notify the downstream
        // publishers because they'll do a SQL query, which will make
        // SQLite throw an exception.
        SQLiteQueue.async { [weak self] in self?.downstreamSubject.send(value) }
    }

    private var touch: (URL?) -> Void {
        { [id] url in
            guard let url else { return }
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
