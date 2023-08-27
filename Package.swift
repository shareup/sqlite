// swift-tools-version:5.8
import PackageDescription

let package = Package(
    name: "SQLite",
    platforms: [
        .macOS(.v13), .iOS(.v16), .tvOS(.v16), .watchOS(.v9),
    ],
    products: [
        .library(
            name: "SQLite",
            targets: ["SQLite"]
        ),
    ],
    dependencies: [
        .package(
            url: "https://github.com/shareup/combine-extensions.git",
            from: "5.0.2"
        ),
        .package(
            url: "https://github.com/groue/GRDB.swift.git",
            from: "6.17.0"
        ),
        .package(
            url: "https://github.com/shareup/precise-iso-8601-date-formatter.git",
            from: "1.0.3"
        ),
        .package(
            url: "https://github.com/shareup/synchronized.git",
            from: "4.0.1"
        ),
    ],
    targets: [
        .target(
            name: "SQLite",
            dependencies: [
                .product(name: "GRDB", package: "GRDB.swift"),
                .product(
                    name: "PreciseISO8601DateFormatter",
                    package: "precise-iso-8601-date-formatter"
                ),
                .product(name: "Synchronized", package: "synchronized"),
            ]
        ),
        .testTarget(
            name: "SQLiteTests",
            dependencies: [
                .product(name: "CombineExtensions", package: "combine-extensions"),
                .product(name: "CombineTestExtensions", package: "combine-extensions"),
                "SQLite",
            ]
        ),
    ]
)
