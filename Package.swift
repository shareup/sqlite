// swift-tools-version:5.3
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
        .package(
            name: "CombineExtensions",
            url: "https://github.com/shareup/combine-extensions.git",
            from: "4.0.0"
        ),
        .package(
            name: "Synchronized",
            url: "https://github.com/shareup/synchronized.git",
            from: "3.0.0"
        ),
    ],
    targets: [
        .target(
            name: "SQLite",
            dependencies: ["Synchronized"]),
        .testTarget(
            name: "SQLiteTests",
            dependencies: [
                "CombineExtensions",
                .product(name: "CombineTestExtensions", package: "CombineExtensions"),
                "SQLite",
            ]),
    ]
)
