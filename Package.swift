// swift-tools-version:5.1
import PackageDescription

let package = Package(
    name: "SQLite",
    platforms: [
        .macOS(.v10_15), .iOS(.v13), .tvOS(.v13), .watchOS(.v5),
    ],
    products: [
        .library(
            name: "SQLite",
            targets: ["SQLite"]),
    ],
    dependencies: [
        .package(url: "https://github.com/shareup/atomic.git", .upToNextMajor(from: "1.0.0"))
    ],
    targets: [
        .target(
            name: "SQLite",
            dependencies: [
                "Atomic"
            ]),
        .testTarget(
            name: "SQLiteTests",
            dependencies: ["SQLite"]),
    ]
)
