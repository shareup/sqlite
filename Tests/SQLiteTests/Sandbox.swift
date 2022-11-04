import Foundation

final class Sandbox {
    var path: String { url.path }

    private let url: URL

    static func execute(_ block: (URL) throws -> Void) throws {
        let sandbox = try Sandbox()
        try block(sandbox.url)
    }

    init() throws {
        url = Sandbox.temporaryDirectory
        try createDirectory(at: url)
    }

    deinit {
        removeDirectory(at: path)
    }
}

private extension Sandbox {
    static var temporaryDirectory: URL {
        FileManager.default.temporaryDirectory.appendingPathComponent("\(arc4random())")
    }

    func createDirectory(at url: URL) throws {
        let fileManager = FileManager.default
        if fileManager.fileExists(atPath: url.path) {
            try fileManager.removeItem(at: url)
        }
        try fileManager.createDirectory(at: url, withIntermediateDirectories: true)
    }

    func removeDirectory(at path: String) {
        let fileManager = FileManager.default
        if fileManager.fileExists(atPath: path) {
            try? fileManager.removeItem(atPath: path)
        }
    }
}
