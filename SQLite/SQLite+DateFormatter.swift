import Foundation

extension SQLite {
    // `ISO8601DateFormatter` does not maintain nanosecond precision, which makes it
    // nearly impossible to equate encodable objects that include `Date` properties.
    // `SQLite.DateFormatter` maintains nanosecond precision by storing the exact
    // bit pattern of `Date.timeIntervalSinceReferenceDate`, which is the type's
    // underlying primitive. https://developer.apple.com/documentation/foundation/nsdate
    public struct DateFormatter {
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
}
