import Foundation
import SQLite

struct Transformable: Equatable, SQLiteTransformable {
    static var tableName: String { "transformables" }

    let name: String
    let age: Int
    let jobTitle: String?

    init(name: String, age: Int, jobTitle: String? = nil) {
        self.name = name
        self.age = age
        self.jobTitle = jobTitle
    }

    init(row: SQLiteRow) throws {
        name = try row.value(for: "name")
        age = try row.value(for: "age")
        jobTitle = try row.value(for: "title")
    }

    var asArguments: SQLiteArguments {
        [
            "name": .text(name),
            "age": .integer(Int64(age)),
            "title": jobTitle == nil ? .null : .text(jobTitle!),
        ]
    }
}

extension Transformable {
    static var createTable: SQL {
        """
        CREATE TABLE transformables (
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            title TEXT
        );
        """
    }

    static var fetchAll: SQL {
        "SELECT * FROM transformables;"
    }

    static var fetchByName: SQL {
        "SELECT * FROM transformables WHERE name=:name;"
    }

    static var insert: SQL {
        "INSERT INTO transformables VALUES (:name, :age, :title);"
    }
}
