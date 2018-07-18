# SQLite.Database

[![Swift](https://img.shields.io/badge/swift-4.2-green.svg?longCache=true&style=flat)](https://developer.apple.com/swift/)
[![License](https://img.shields.io/badge/license-MIT-green.svg?longCache=true&style=flat)](/LICENSE)
[![Build status](https://build.appcenter.ms/v0.1/apps/1d373ab3-f5f7-4718-ab3f-5223c3cfa97f/branches/master/badge)](https://appcenter.ms)

## Introduction

[SQLite.Database]() is a simple Swift wrapper around [SQLite](http://www.sqlite.org/). It is intended to act as an introduction to SQLite for Swift developers. You can read more about that [here](https://shareup.app/blog/building-a-lightweight-sqlite-wrapper-in-swift/). SQLite.Database allows developers to use pure SQL to access or modify their database without forcing them to deal with all the tiresome minutia involved in configuring SQLite databases and queries or converting from Swift types to the C types that SQLite expects.

## Installation

### Carthage

1. [Install Carthage](https://github.com/Carthage/Carthage#installing-carthage)
2. Add the following to your Cartfile:

```
github "shareup-app/sqlite" ~> 1.0.0
```
3. Run `carthage update` and [add the correct framework](https://github.com/Carthage/Carthage#adding-frameworks-to-an-application) to your application

### Manually

1. [Download the most recent tagged version of SQLite.Database](https://github.com/shareup-app/sqlite/releases/tag/v1.0)
2. Add `SQLite.xcodeproj` to your workspace (.xcworkspace file) by clicking on "Add Files to [your workspace]". Select `SQLite.xcodeproj` and click "Add."
3. If you have not already done so, add a "Copy Frameworks" build phase to your app's target. Select your app's project file in Xcode's sidebar and then click on the "Build Phases" tab. Click the + button above "Target Dependencies" for your app's target. Choose "New Copy Files Phase". Rename the newly-created phase to "Copy Frameworks". Change the destination from "Resources" to "Frameworks".
4. To add the new library to your app, if you're not already there, select your app's project file in Xcode's sidebar. Click on the "Build Phases" tab and open the "Copy Frameworks" section. Drag the "SQLite.framework" file from inside the SQLite framework's "Products" group to the "Copy Frameworks" . After doing this, you should see the "SQLite.framework" show up in the sidebar under your app's frameworks group. Click on the framework, show the Utilities (the third pane on the right side in Xcode), and verify that the framework's location is "Relative to Build Products".

## License

The license for SQLite.Database is the standard MIT licence. You can find it in the `LICENSE` file.

## Alternatives

- [GRDB](https://github.com/groue/GRDB.swift)
- [SQLite.swift](https://github.com/stephencelis/SQLite.swift)
