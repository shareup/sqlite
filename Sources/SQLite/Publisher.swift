import Foundation
import Combine
import Atomic

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
            return subscriber.receive(completion: .failure(SQLiteError.onSubscribeWithoutDatabase))
        }

        do {
            let subscription = Subscription(subscriber: AnySubscriber(subscriber))
            try subscription.observe(_sql, arguments: _arguments, queue: _queue, on: database)
            subscriber.receive(subscription: subscription)
        } catch {
            subscriber.receive(completion: .failure(error))
        }
    }
}

final class Subscription: Combine.Subscription {
    private let _subscriber: AnySubscriber<Array<SQLiteRow>, Swift.Error>

    private var _demand: Subscribers.Demand = .none

    @Atomic(nil) private var _token: AnyObject?

    init(subscriber: AnySubscriber<Array<SQLiteRow>, Swift.Error>) {
        _subscriber = subscriber
    }

    func request(_ demand: Subscribers.Demand) {
        _demand += demand
    }

    func cancel() {
        _token = nil
        _demand = .none
    }

    func observe(_ sql: SQL, arguments: SQLiteArguments, queue: DispatchQueue, on database: Database) throws {
        let block = { (rows: Array<SQLiteRow>) -> Void in
            queue.async { [weak self] in
                self?.receive(rows)
            }
        }

        _token = try database.observe(sql, arguments: arguments, queue: queue, block: block)
    }

    func receive(_ rows: Array<SQLiteRow>) {
        guard _token != nil else { return }
        guard _demand > 0 else { return }
        _demand -= rows.count
        _demand += _subscriber.receive(rows)
    }
}
