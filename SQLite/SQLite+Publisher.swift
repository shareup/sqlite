import Foundation
import Combine

extension SQLite {
    final class Publisher: Combine.Publisher {
        typealias Output = Array<SQLiteRow>
        typealias Failure = Swift.Error

        private weak var _database: Database?
        private let _sql: SQL
        private let _arguments: SQLiteArguments
        private let _queue: DispatchQueue

        private let _lock = Lock()
        private var _subscriptions: Set<Subscription> = []

        init(database: Database, sql: SQL, arguments: SQLiteArguments = [:], queue: DispatchQueue = .main) {
            _database = database
            _sql = sql
            _arguments = arguments
            _queue = queue
        }

        public func receive<S: Subscriber>(subscriber: S) where Failure == S.Failure, Output == S.Input {
            guard let database = _database else {
                return subscriber.receive(completion: .failure(SQLite.Error.onSubscribeWithoutDatabase))
            }

            do {
                let block: (Array<SQLiteRow>) -> Void = { [weak self] in self?.onUpdate(rows: $0) }
                let token = try database.observe(_sql, arguments: _arguments, queue: _queue, block: block)
                let subscription = Subscription(subscriber: subscriber.eraseToAnySubscriber(), token: token)
                _lock.locked { _subscriptions.insert(subscription) }
                subscriber.receive(subscription: subscription)
            } catch {
                subscriber.receive(completion: .failure(error))
            }
        }

        private func onUpdate(rows: Array<SQLiteRow>) {
            _lock.locked {
                _subscriptions.forEach { subscription in
                    _queue.async {
                        subscription.receive(rows)
                        subscription.request(.unlimited)
                    }
                }
            }
        }
    }
}

private extension SQLite {
    final class Subscription: Combine.Subscription, Hashable {
        private let _subscriber: AnySubscriber<Array<SQLiteRow>, Swift.Error>
        private var _token: AnyObject?
        private var _demand: Subscribers.Demand?

        init(subscriber: AnySubscriber<Array<SQLiteRow>, Swift.Error>, token: AnyObject) {
            _subscriber = subscriber
            _token = token
        }

        func request(_ demand: Subscribers.Demand) {
            _demand = demand
        }

        func cancel() {
            _token = nil
        }

        func receive(_ rows: Array<SQLiteRow>) {
            guard _token != nil else { return }

            if let max = _demand?.max, rows.count > max {
                _demand = _subscriber.receive(Array(rows.prefix(max)))
            } else {
                _demand = _subscriber.receive(rows)
            }
        }

        func hash(into hasher: inout Hasher) {
            hasher.combine(_subscriber.combineIdentifier)
        }

        var description: String {
            return _subscriber.combineIdentifier.description
        }

        static func == (lhs: Subscription, rhs: Subscription) -> Bool {
            return lhs._subscriber.combineIdentifier == rhs._subscriber.combineIdentifier
        }
    }
}
