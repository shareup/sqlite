import Combine
import Foundation
import SQLite3
import Synchronized

extension Publisher where Output == SQLiteDatabaseChange, Failure == Never {
    func results(
        sql: SQL,
        arguments: SQLiteArguments,
        tables: [String],
        database: SQLiteDatabase
    ) -> SQLiteResultsPublisher<Self> {
        SQLiteResultsPublisher(
            upstream: self,
            sql: sql,
            arguments: arguments,
            tables: tables,
            database: database
        )
    }
}

struct SQLiteResultsPublisher<Upstream: Publisher>: Publisher where
    Upstream.Output == SQLiteDatabaseChange, Upstream.Failure == Never
{
    typealias Output = [SQLiteRow]
    typealias Failure = SQLiteError

    private let upstream: Upstream
    private let sql: SQL
    private let arguments: SQLiteArguments
    private let tables: [String]
    private weak var database: SQLiteDatabase?

    init(
        upstream: Upstream,
        sql: SQL,
        arguments: SQLiteArguments = [:],
        tables: [String] = [],
        database: SQLiteDatabase
    ) {
        self.upstream = upstream
        self.sql = sql
        self.arguments = arguments
        self.tables = tables
        self.database = database
    }

    func receive<S: Subscriber>(
        subscriber: S
    ) where Failure == S.Failure, Output == S.Input {
        let subscription = SQLiteStatementResultsSubscription(
            sql: sql,
            arguments: arguments,
            tables: tables,
            database: database,
            subscriber: subscriber
        )
        upstream.subscribe(subscription)
    }
}

private final class SQLiteStatementResultsSubscription<S>: Subscription, Subscriber where
    S: Subscriber, S.Input == [SQLiteRow], S.Failure == SQLiteError
{
    typealias Input = SQLiteDatabaseChange
    typealias Failure = Never

    private enum State {
        case waitingForSubscription
        case subscribedToChanges(Subscription, SQLiteStatement, Set<String>)
        case paused(Subscription)
        case completed

        mutating func pause() {
            switch self {
            case .waitingForSubscription, .paused, .completed:
                break

            case let .subscribedToChanges(subscription, statement, _):
                sqlite3_finalize(statement)
                self = .paused(subscription)
            }
        }

        mutating func cancel() {
            switch self {
            case .waitingForSubscription, .completed:
                self = .completed

            case let .subscribedToChanges(subscription, statement, _):
                subscription.cancel()
                sqlite3_finalize(statement)
                self = .completed

            case let .paused(subscription):
                subscription.cancel()
                self = .completed
            }
        }
    }

    private var subscriber: S?
    private let sql: SQL
    private let arguments: SQLiteArguments
    private let tables: Set<String>
    private weak var database: SQLiteDatabase?

    private var changeKey: Int = 0
    private var lastPublishedChangeKey: Int = -1

    private var state: State = .waitingForSubscription
    private let lock = RecursiveLock()
    private var demand = Subscribers.Demand.none

    init(
        sql: SQL,
        arguments: SQLiteArguments,
        tables: [String],
        database: SQLiteDatabase?,
        subscriber: S
    ) {
        self.subscriber = subscriber
        self.sql = sql
        self.arguments = arguments
        self.tables = Set(tables)
        self.database = database
    }

    deinit {
        cancel()
    }

    func request(_ demand: Subscribers.Demand) {
        guard demand != .none else { return }

        let _subscription: Subscription? = lock.locked {
            switch state {
            case .waitingForSubscription, .completed:
                return nil

            case let .subscribedToChanges(subscription, _, _):
                self.demand += demand
                return subscription

            case let .paused(subscription):
                self.demand += demand
                return subscription
            }
        }

        guard _subscription != nil else { return }
        publish()
    }

    func cancel() {
        lock.locked {
            subscriber = nil
            state.cancel()
        }
    }

    func receive(subscription: Subscription) {
        let next: () -> Void = lock.locked {
            guard let sub = subscriber else { return { subscription.cancel() } }
            guard let db = database else {
                subscriber = nil
                state.cancel()
                return {
                    subscription.cancel()
                    sub.receive(completion: .failure(.onSubscribeWithoutDatabase))
                }
            }

            switch state {
            case .waitingForSubscription:
                do {
                    let statement = try prepareAndBindStatement(database: db)
                    state = .subscribedToChanges(subscription, statement, tables)
                    return {
                        subscription.request(.unlimited)
                        sub.receive(subscription: self)
                    }
                } catch let sqliteError as SQLiteError {
                    subscriber = nil
                    state.cancel()
                    return {
                        subscription.cancel()
                        sub.receive(completion: .failure(sqliteError))
                    }
                } catch {
                    subscriber = nil
                    state.cancel()
                    return {
                        subscription.cancel()
                        sub.receive(completion: .failure(.onInternalError(error as NSError)))
                    }
                }

            case .subscribedToChanges, .paused, .completed:
                return { subscription.cancel() }
            }
        }

        next()
    }

    func receive(_ input: SQLiteDatabaseChange) -> Subscribers.Demand {
        let next: (() -> Void)? = lock.locked {
            changeKey = changeKey &+ 1

            switch state {
            case .waitingForSubscription, .completed:
                return nil

            case let .subscribedToChanges(_, _, tables):
                switch input {
                case .open:
                    // This shouldn't be possible.
                    return nil

                case .close:
                    state.pause()
                    return nil

                case .crossProcessUpdate:
                    return { self.publish() }

                case let .updateTables(updatedTables):
                    guard tables.isEmpty || !tables.isDisjoint(with: updatedTables)
                    else { return nil }
                    return { self.publish() }

                case .updateAllTables:
                    return { self.publish() }
                }

            case let .paused(subscription):
                guard case .open = input else { return nil }
                guard let db = database, let sub = subscriber else {
                    let sub = subscriber
                    subscriber = nil
                    state.cancel()
                    return {
                        subscription.cancel()
                        sub?.receive(completion: .failure(.onSubscribeWithoutDatabase))
                    }
                }

                do {
                    let statement = try prepareAndBindStatement(database: db)
                    state = .subscribedToChanges(subscription, statement, tables)
                    return { self.publish() }
                } catch let sqliteError as SQLiteError {
                    subscriber = nil
                    state.cancel()
                    return {
                        subscription.cancel()
                        sub.receive(completion: .failure(sqliteError))
                    }
                } catch {
                    subscriber = nil
                    state.cancel()
                    return {
                        subscription.cancel()
                        sub.receive(completion: .failure(.onInternalError(error as NSError)))
                    }
                }
            }
        }
        next?()
        return .none // We requested .unlimited when first subscribing
    }

    func receive(completion _: Subscribers.Completion<Never>) {
        let _subscriber: S? = lock.locked {
            state.cancel()
            let sub = subscriber
            subscriber = nil
            return sub
        }

        _subscriber?.receive(completion: .finished)
    }

    private func prepareAndBindStatement(database: SQLiteDatabase) throws -> SQLiteStatement {
        let statement = try SQLiteStatement.preparePersistent(sql, in: database)
        try statement.bind(arguments: arguments)
        return statement
    }

    private func publish() {
        lock.locked {
            guard demand > .none else { return }
            guard let subscriber = self.subscriber else { return }
            guard let _ = database else { return }
            guard lastPublishedChangeKey != changeKey else { return }

            switch state {
            case .waitingForSubscription, .paused, .completed:
                break

            case let .subscribedToChanges(_, statement, _):
                // Using a standard `defer { statement.reset() ` can cause a
                // crash if `subscriber.receive(result)` results in the publisher
                // being cancelled because cancelling the publisher calls
                // `state.cancel()`, which finalizes the statement, which will
                // then cause the app to crash when `statement.reset()` is called.
                // So, we need to make sure the reset happens as soon as the statement
                // is evaluated.
                let result: [SQLiteRow]
                do {
                    defer { statement.reset() }
                    guard let r = try? statement.evaluate() else { return }
                    result = r.1
                }

                demand -= .max(1)
                lastPublishedChangeKey = changeKey
                let newDemand = subscriber.receive(result)
                guard newDemand != .none else { return }
                demand += newDemand
            }
        }
    }
}
