import Combine
import Foundation
import Synchronized

final class CrossProcessChangeNotifier: NSObject, @unchecked Sendable {
    private let id = UUID().uuidString

    private let changeTrackerURL: URL?
    private let isStarted = Locked<Bool>(false)

    private let localChange = PassthroughSubject<Void, Never>()
    private var localChangeSubscription: AnyCancellable?

    private let remoteChange = PassthroughSubject<Void, Never>()
    private var remoteChangeSubscription: AnyCancellable?

    private let queue: OperationQueue = {
        let queue = OperationQueue()
        queue.name = "app.shareup.sqlite.cross-process-change-notifier"
        queue.maxConcurrentOperationCount = 1
        queue.qualityOfService = .default
        queue.underlyingQueue = .global()
        return queue
    }()

    init(
        databasePath: String,
        onRemoteChange: @Sendable @escaping () -> Void
    ) {
        changeTrackerURL = Self.changeTrackerURL(
            databasePath: databasePath
        )

        super.init()

        localChangeSubscription = localChange.throttle(
            for: .seconds(1),
            scheduler: RunLoop.main,
            latest: true
        )
        .receive(on: queue)
        .sink { [weak self] _ in
            guard let self, let url = changeTrackerURL else { return }
            let coordinator = NSFileCoordinator(filePresenter: self)
            coordinator.coordinate(
                writingItemAt: url,
                options: .forReplacing,
                error: nil,
                byAccessor: touch
            )
        }

        remoteChangeSubscription = remoteChange.throttle(
            for: .seconds(1),
            scheduler: RunLoop.main,
            latest: true
        )
        .receive(on: queue)
        .sink { onRemoteChange() }
    }

    func start() {
        let needsRegistration = isStarted.access { isStarted in
            guard !isStarted else { return false }
            isStarted = true
            return true
        }
        guard needsRegistration else { return }
        registerFilePresenter()
    }

    func stop() {
        let needsRemoval = isStarted.access { isStarted in
            guard isStarted else { return false }
            isStarted = false
            return true
        }
        guard needsRemoval else { return }
        removeFilePresenter()
    }
}

extension CrossProcessChangeNotifier: NSFilePresenter {
    var presentedItemURL: URL? { changeTrackerURL }
    var presentedItemOperationQueue: OperationQueue { queue }

    func presentedItemDidChange() {
        remoteChange.send()
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
        localChange.send()
    }

    private var touch: (URL?) -> Void {
        { [id] url in
            guard let url else { return }
            try? id.write(to: url, atomically: true, encoding: .utf8)
        }
    }

    private static func changeTrackerURL(databasePath: String) -> URL? {
        guard databasePath != ":memory:" else { return nil }
        return URL(
            fileURLWithPath: databasePath.appending("-change-tracker"),
            isDirectory: false
        )
    }
}
