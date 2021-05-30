# SQLite

[![Swift](https://img.shields.io/badge/swift-5.2-green.svg?longCache=true&style=flat)](https://developer.apple.com/swift/)
[![License](https://img.shields.io/badge/license-MIT-green.svg?longCache=true&style=flat)](/LICENSE)
![Build](https://github.com/shareup/sqlite/workflows/Build/badge.svg)

## Introduction

`SQLite` is a simple Swift wrapper around [SQLite](http://www.sqlite.org/). It is intended to act as an introduction to SQLite for Swift developers. You can read more about that [here](https://shareup.app/blog/building-a-lightweight-sqlite-wrapper-in-swift/). Database allows developers to use pure SQL to access or modify their database without forcing them to deal with all the tiresome minutia involved in configuring SQLite databases and queries or converting from Swift types to the C types that SQLite expects.

## Installation

### Swift Package Manager

To use SQLite with the Swift Package Manager, add a dependency to your Package.swift file:

```swift
let package = Package(
  dependencies: [
    .package(url: "https://github.com/shareup/sqlite.git", .upToNextMajor(from: "16.0.0"))
  ]
)
```

## License

The license for Database is the standard MIT licence. You can find it in the `LICENSE` file.

## Alternatives

- [GRDB](https://github.com/groue/GRDB.swift)
- [SQLite.swift](https://github.com/stephencelis/SQLite.swift)
