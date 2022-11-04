import Foundation
import PreciseISO8601DateFormatter

// `ISO8601DateFormatter` does not maintain microsecond precision, which makes it
// nearly impossible to equate encodable objects that include `Date` properties.
// `DateFormatter` maintains microsecond precision.
public enum PreciseDateFormatter {
    public static func string(from date: Date) -> String {
        formatter.string(from: date)
    }

    public static func date(from string: String) -> Date? {
        formatter.date(from: string)
    }
}

public extension KeyedDecodingContainer {
    func decodePreciseDate(forKey key: K) throws -> Date {
        let asString = try decode(String.self, forKey: key)
        guard let date = PreciseDateFormatter.date(from: asString) else {
            let context = DecodingError.Context(
                codingPath: codingPath,
                debugDescription: "Could not parse '\(asString)' into Date."
            )
            throw Swift.DecodingError.typeMismatch(Date.self, context)
        }
        return date
    }

    func decodePreciseDateIfPresent(forKey key: K) throws -> Date? {
        guard let asString = try decodeIfPresent(String.self, forKey: key) else { return nil }
        guard let date = PreciseDateFormatter.date(from: asString) else {
            let context = DecodingError.Context(
                codingPath: codingPath,
                debugDescription: "Could not parse '\(asString)' into Date."
            )
            throw Swift.DecodingError.typeMismatch(Date.self, context)
        }
        return date
    }
}

public extension KeyedEncodingContainer {
    mutating func encode(preciseDate: Date, forKey key: K) throws {
        try encode(PreciseDateFormatter.string(from: preciseDate), forKey: key)
    }

    mutating func encodeIfPresent(preciseDate: Date?, forKey key: K) throws {
        guard let preciseDate else { return }
        try encodeIfPresent(PreciseDateFormatter.string(from: preciseDate), forKey: key)
    }
}

private let formatter = PreciseISO8601DateFormatter()
