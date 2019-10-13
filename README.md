# SQLite.Database

[![Swift](https://img.shields.io/badge/swift-5.1-green.svg?longCache=true&style=flat)](https://developer.apple.com/swift/)
[![License](https://img.shields.io/badge/license-MIT-green.svg?longCache=true&style=flat)](/LICENSE)

## Introduction

[SQLite.Database]() is a simple Swift wrapper around [SQLite](http://www.sqlite.org/). It is intended to act as an introduction to SQLite for Swift developers. You can read more about that [here](https://shareup.app/blog/building-a-lightweight-sqlite-wrapper-in-swift/). SQLite.Database allows developers to use pure SQL to access or modify their database without forcing them to deal with all the tiresome minutia involved in configuring SQLite databases and queries or converting from Swift types to the C types that SQLite expects.

## Installation

### Swift Package Manager

To use SQLite with the Swift Package Manager, add a dependency to your Package.swift file:

```swift
let package = Package(
  dependencies: [
    .package(url: "https://github.com/shareup/sqlite.git", .upToNextMajor(from: "7.0.0"))
  ]
)
```

## License

The license for SQLite.Database is the standard MIT licence. You can find it in the `LICENSE` file.

## Alternatives

- [GRDB](https://github.com/groue/GRDB.swift)
- [SQLite.swift](https://github.com/stephencelis/SQLite.swift)
