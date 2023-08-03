import Foundation
import os.log

let log = OSLog(
    subsystem: Bundle.main.bundleIdentifier ?? "app.shareup.sqlite",
    category: "sqlite"
)
