import Combine
import Foundation
import Synchronized

struct SQLiteFuture<Output>: Publisher {
    typealias Failure = SQLiteError

    typealias Promise = (Result<Output, Failure>) -> Void

    private let attemptToFulfill: (@escaping Promise) -> Void

    init(
        _ attemptToFulfill: @escaping (@escaping Promise) -> Void
    ) {
        self.attemptToFulfill = attemptToFulfill
    }

    func receive<S: Subscriber>(
        subscriber: S
    ) where Failure == S.Failure, Output == S.Input {
        let subscription = SQLiteFutureSubscription(
            attemptToFulfill: attemptToFulfill,
            subscriber: subscriber
        )
        subscriber.receive(subscription: subscription)
    }
}

private final class SQLiteFutureSubscription<Output, S: Subscriber>: Subscription
    where
    S.Input == Output,
    S.Failure == SQLiteError
{
    private enum State {
        case pending
        case fulfilled(Result<S.Input, S.Failure>)
        case finished
    }

    private var state: State = .pending
    private var hasDemand = false

    private var subscriber: S?
    private let lock = Lock()

    init(
        attemptToFulfill: (@escaping SQLiteFuture<Output>.Promise) -> Void,
        subscriber: S
    ) {
        self.subscriber = subscriber
        attemptToFulfill { result in self.fulfill(with: result) }
    }

    deinit {
        lock.locked { subscriber = nil }
    }

    func request(_ demand: Subscribers.Demand) {
        guard demand != .none else { return }

        let subscriberAndResult: (S, Result<S.Input, S.Failure>)? = lock.locked {
            hasDemand = true
            guard let sub = subscriber, case let .fulfilled(result) = state
            else { return nil }
            state = .finished
            subscriber = nil
            return (sub, result)
        }

        guard let (subscriber, result) = subscriberAndResult else { return }
        notify(subscriber: subscriber, result: result)
    }

    func cancel() {
        lock.locked {
            subscriber = nil
            state = .finished
        }
    }

    private func fulfill(with result: Result<S.Input, S.Failure>) {
        let _subscriber: S? = lock.locked {
            guard case .pending = state, let sub = subscriber
            else { return nil }

            if hasDemand {
                state = .finished
                subscriber = nil
                return sub
            } else {
                state = .fulfilled(result)
                return nil
            }
        }

        guard let subscriber = _subscriber else { return }
        notify(subscriber: subscriber, result: result)
    }

    private func notify(subscriber: S, result: Result<S.Input, S.Failure>) {
        switch result {
        case let .success(rows):
            _ = subscriber.receive(rows)
            subscriber.receive(completion: .finished)

        case let .failure(error):
            subscriber.receive(completion: .failure(error))
        }
    }
}
