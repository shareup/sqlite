import Foundation
import Combine
import Synchronized

class SQLiteFuture<Output>: Publisher {
    typealias Failure = SQLiteError

    typealias Promise = (Result<Output, Failure>) -> Void

    private enum State {
        case notStarted((@escaping SQLiteFuture.Promise) -> Void)
        case inProgress
        case failed(SQLiteError)
        case finished(Output)

        var result: Result<Output, SQLiteError>? {
            switch self {
            case .notStarted, .inProgress:
                return nil

            case let .failed(error):
                return .failure(error)

            case let .finished(output):
                return .success(output)
            }
        }
    }

    private var state: State
    private var subscriptions = Set<SQLiteFutureSubscription<Output>>()
    private var lock = RecursiveLock()

    init(
        _ attemptToFulfill: @escaping (@escaping Promise) -> Void
    ) {
        self.state = .notStarted(attemptToFulfill)
    }

    func receive<S: Subscriber>(
        subscriber: S
    ) where Failure == S.Failure, Output == S.Input {
        let subscription = SQLiteFutureSubscription(
            subscriber: subscriber,
            resultProvider: resultProvider
        )
        lock.locked { let _ = subscriptions.insert(subscription) }
        subscriber.receive(subscription: subscription)
    }

    private func notifySubscribers() {
        lock.locked {
            guard let result = state.result else { return }

            var notified = Set<SQLiteFutureSubscription<Output>>()
            for sub in subscriptions {
                if sub.sendResult(result) {
                    notified.insert(sub)
                }
            }
            subscriptions.subtract(notified)
        }
    }

    enum NextAction {
        case doNothing
        case attemptToFulfill((@escaping Promise) -> Void)
        case sendOutput(Output)
        case sendFailure(SQLiteError)
    }

    private var resultProvider: () -> Result<Output, SQLiteError>? {
        { [weak self] in
            guard let self = self else { return nil }

            let nextAction = self.lock.locked { () -> NextAction in
                switch self.state {
                case let .notStarted(attemptToFulfill):
                    self.state = .inProgress
                    return .attemptToFulfill(attemptToFulfill)

                case .inProgress:
                    return .doNothing

                case let .finished(output):
                    return .sendOutput(output)

                case let .failed(error):
                    return .sendFailure(error)
                }
            }

            switch nextAction {
            case let .attemptToFulfill(attemptToFulfill):
                // We can't use `[weak self]` here or the block
                // will be deallocated immediately after invoked.
                // This should not create a retain cycle because
                // `self` does not retain the block.
                attemptToFulfill({ result in
                    self.lock.locked {
                        guard case .inProgress = self.state else { return }

                        switch result {
                        case let .success(output):
                            self.state = .finished(output)

                        case let .failure(error):
                            self.state = .failed(error)
                        }
                    }

                    self.notifySubscribers()
                })
                return nil

            case .doNothing:
                return nil

            case let .sendOutput(output):
                return .success(output)

            case let .sendFailure(error):
                return .failure(error)
            }
        }
    }
}

private final class SQLiteFutureSubscription<Output>: Subscription, Hashable {
    let id: CombineIdentifier

    private var hasDemand = false
    private var subscriber: AnySubscriber<Output, SQLiteError>?
    private let resultProvider: () -> Result<Output, SQLiteError>?
    private let lock = RecursiveLock()

    init<S: Subscriber>(
        subscriber: S,
        resultProvider: @escaping () -> Result<Output, SQLiteError>?
    ) where S.Input == Output, S.Failure == SQLiteError {
        self.id = subscriber.combineIdentifier
        self.subscriber = AnySubscriber(subscriber)
        self.resultProvider = resultProvider
    }

    deinit {
        lock.locked { subscriber = nil }
    }

    func request(_ demand: Subscribers.Demand) {
        guard demand != .none else { return }
        lock.locked { self.hasDemand = true }
        guard let result = resultProvider() else { return }
        let _ = sendResult(result)
    }

    func cancel() {
        lock.locked { subscriber = nil }
    }

    func sendResult(_ result: Result<Output, SQLiteError>) -> Bool {
        lock.locked { () -> Bool in
            // If we don't have any demand, we return `false` so that
            // `SQLiteFuture` holds on to this subscription until it
            // receives some demand and calls `resultProvider()`.
            guard hasDemand else { return false }

            // If we don't have a subscriber, we want to be removed from
            // `SQLiteFuture`'s set of subscriptions. So, we need to
            // return `true`.
            guard let sub = subscriber else { return true }
            subscriber = nil

            switch result {
            case let .success(output):
                let _ = sub.receive(output)
                sub.receive(completion: .finished)

            case let .failure(error):
                sub.receive(completion: .failure(error))
            }

            return true
        }
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }

    static func == (lhs: SQLiteFutureSubscription, rhs: SQLiteFutureSubscription) -> Bool {
        lhs.id == rhs.id
    }
}
