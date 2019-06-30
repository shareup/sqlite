import Foundation
import Combine
import Atomic

extension SQLite {
    struct Publisher: Combine.Publisher {
        typealias Output = Array<SQLiteRow>
        typealias Failure = Swift.Error

        private weak var _database: Database?
        private let _sql: SQL
        private let _arguments: SQLiteArguments
        private let _queue: DispatchQueue

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
                let subscription = Subscription(subscriber: subscriber.eraseToAnySubscriber())
                try subscription.observe(_sql, arguments: _arguments, queue: _queue, on: database)
                subscriber.receive(subscription: subscription)
            } catch {
                subscriber.receive(completion: .failure(error))
            }
        }
    }
}

private extension SQLite {
    final class Subscription: Combine.Subscription {
        private let _subscriber: AnySubscriber<Array<SQLiteRow>, Swift.Error>

        private var _demand: Subscribers.Demand?

        @Atomic(nil) private var _token: AnyObject?

        init(subscriber: AnySubscriber<Array<SQLiteRow>, Swift.Error>) {
            _subscriber = subscriber
        }

        func request(_ demand: Subscribers.Demand) {
            _demand = demand
        }

        func cancel() {
            _token = nil
        }

        func observe(_ sql: SQL, arguments: SQLiteArguments, queue: DispatchQueue, on database: Database) throws {
            let block = { (rows: Array<SQLiteRow>) -> Void in
                queue.async { [weak self] in
                    self?.receive(rows)
                    self?.request(.unlimited)
                }
            }

            _token = try database.observe(sql, arguments: arguments, queue: queue, block: block)
        }

        func receive(_ rows: Array<SQLiteRow>) {
            guard _token != nil else { return }

            if let max = _demand?.max, rows.count > max {
                _demand = _subscriber.receive(Array(rows.prefix(max)))
            } else {
                _demand = _subscriber.receive(rows)
            }
        }
    }
}
