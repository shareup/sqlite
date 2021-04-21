import Foundation

struct SQLiteQueue {
    static var isCurrentQueue: Bool {
        DispatchQueue.getSpecific(key: key) == context
    }

    static func async(_ block: @escaping () -> Void) {
        queue.async(execute: block)
    }

    static func sync<T>(_ block: () throws -> T) rethrows -> T {
        if isCurrentQueue {
            return try block()
        } else {
            return try queue.sync(execute: block)
        }
    }
}

private let key = DispatchSpecificKey<Int>()
private let context = Int(arc4random())

private let queue: DispatchQueue = {
    let queue = DispatchQueue(
        label: "app.shareup.sqlite.sqlitedatabase",
        qos: .default,
        attributes: [],
        autoreleaseFrequency: .workItem,
        target: .global()
    )
    queue.setSpecific(key: key, value: context)
    return queue
}()
