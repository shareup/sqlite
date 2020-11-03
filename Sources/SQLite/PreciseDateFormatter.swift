import Foundation

// `ISO8601DateFormatter` does not maintain nanosecond precision, which makes it
// nearly impossible to equate encodable objects that include `Date` properties.
// `DateFormatter` maintains nanosecond precision by storing the exact
// bit pattern of `Date.timeIntervalSinceReferenceDate`, which is the type's
// underlying primitive. https://developer.apple.com/documentation/foundation/nsdate
public struct PreciseDateFormatter {
    public static func string(from date: Date) -> String {
        let bitPattern = date.timeIntervalSinceReferenceDate.bitPattern
        return String(bitPattern)
    }

    public static func date(from string: String) -> Date? {
        guard let bitPattern = UInt64(string) else { return nil }
        let double = Double(bitPattern: bitPattern)
        return Date(timeIntervalSinceReferenceDate: double)
    }
}

extension KeyedDecodingContainer {
    public func decodePreciseDate(forKey key: K) throws -> Date {
        let asString = try self.decode(String.self, forKey: key)
        guard let date = PreciseDateFormatter.date(from: asString) else {
            let context = DecodingError.Context(
                codingPath: self.codingPath,
                debugDescription: "Could not parse '\(asString)' into Date."
            )
            throw Swift.DecodingError.typeMismatch(Date.self, context)
        }
        return date
    }

    public func decodePreciseDateIfPresent(forKey key: K) throws -> Date? {
        guard let asString = try self.decodeIfPresent(String.self, forKey: key) else { return nil }
        guard let date = PreciseDateFormatter.date(from: asString) else {
            let context = DecodingError.Context(
                codingPath: self.codingPath,
                debugDescription: "Could not parse '\(asString)' into Date."
            )
            throw Swift.DecodingError.typeMismatch(Date.self, context)
        }
        return date
    }
}

extension KeyedEncodingContainer {
    public mutating func encode(preciseDate: Date, forKey key: K) throws {
        try self.encode(PreciseDateFormatter.string(from: preciseDate), forKey: key)
    }

    public mutating func encodeIfPresent(preciseDate: Date?, forKey key: K) throws {
        guard let preciseDate = preciseDate else { return }
        try self.encodeIfPresent(PreciseDateFormatter.string(from: preciseDate), forKey: key)
    }
}
