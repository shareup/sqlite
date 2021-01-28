import Combine
import Foundation
import Synchronized

struct SQLitePublisher: Publisher {
    typealias Output = Array<SQLiteRow>
    typealias Failure = Swift.Error

    private weak var _database: SQLiteDatabase?
    private let _sql: SQL
    private let _arguments: SQLiteArguments
    private let _queue: DispatchQueue

    init(
        database: SQLiteDatabase,
        sql: SQL,
        arguments: SQLiteArguments = [:],
        queue: DispatchQueue = .main
    ) {
        _database = database
        _sql = sql
        _arguments = arguments
        _queue = queue
    }

    func receive<S: Subscriber>(subscriber: S) where Failure == S.Failure, Output == S.Input {
        guard let database = _database else {
            return subscriber.receive(completion: .failure(SQLiteError.onSubscribeWithoutDatabase))
        }

        do {
            let subscription = SQLiteSubscription(subscriber: AnySubscriber(subscriber))
            subscriber.receive(subscription: subscription)
            try subscription.observe(_sql, arguments: _arguments, queue: _queue, on: database)
        } catch {
            subscriber.receive(completion: .failure(error))
        }
    }
}

private final class SQLiteSubscription: Subscription {
    private var _subscriber: AnySubscriber<Array<SQLiteRow>, Swift.Error>?

    private let _lock = RecursiveLock()

    private var _demand: Subscribers.Demand = .none
    private var _token: AnyObject?

    init(subscriber: AnySubscriber<Array<SQLiteRow>, Swift.Error>) {
        _subscriber = subscriber
    }

    func request(_ demand: Subscribers.Demand) {
        _lock.locked { _demand += demand }
    }

    func cancel() {
        _lock.locked {
            _subscriber = nil
            _token = nil
            _demand = .none
        }
    }

    func observe(
        _ sql: SQL,
        arguments: SQLiteArguments,
        queue: DispatchQueue,
        on database: SQLiteDatabase
    ) throws {
        let block = { (rows: Array<SQLiteRow>) -> Void in
            queue.async { [weak self] in
                self?.receive(rows)
            }
        }

        let token = try database.observe(sql, arguments: arguments, queue: queue, block: block)
        _lock.locked { _token = token }
    }

    func receive(_ rows: Array<SQLiteRow>) {
        _lock.locked {
            guard _token != nil, _demand > 0 else { return }
            guard let subscriber = _subscriber else { return }

            _demand -= .max(1)
            let newDemand = subscriber.receive(rows)

            // Combine doesn’t treat `Subscribers.Demand.none` as zero and
            // adding or subtracting `.none` will trigger an exception.
            // https://www.raywenderlich.com/books/combine-asynchronous-programming-with-swift/v2.0/chapters/18-custom-publishers-handling-backpressure
            guard newDemand != .none else { return }

            _demand += newDemand
        }
    }
}
