# SQLite

[![Swift](https://img.shields.io/badge/swift-5.2-green.svg?longCache=true&style=flat)](https://developer.apple.com/swift/)
[![License](https://img.shields.io/badge/license-MIT-green.svg?longCache=true&style=flat)](/LICENSE)
![Build](https://github.com/shareup/sqlite/workflows/Build/badge.svg)

## Introduction

SQLite is a simple wrapper around [GRDB](https://github.com/groue/GRDB.swift).

This library started its life as a simple Swift wrapper around [SQLite](http://www.sqlite.org/) focused on allowing clients to subscribe to specific SQL queries and receive updates via Combine publishers whenever the query results changed. As time went on, though, the maintainer of this library focused his attention elsewhere. Meanwhile, GRDB continued to improve. Starting in August 2023, the maintainer of this library decided to replace the majority of its internals with GRDB, while keeping most of the external API and behavior of SQLite consistent. 

## Installation

### Swift Package Manager

To use SQLite with the Swift Package Manager, add a dependency to your Package.swift file:

```swift
let package = Package(
  dependencies: [
    .package(
      url: "https://github.com/shareup/sqlite.git",
      from: "19.0.0"
    )
  ]
)
```

## License

The license for SQLite is the standard MIT licence. You can find it in the `LICENSE` file.

## GRDB License

The license for GRDB is the standard MIT license. You can find it [here](https://github.com/groue/GRDB.swift/blob/master/LICENSE).
