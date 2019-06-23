import Foundation
import SQLite

struct Transformable: Equatable, SQLiteTransformable {
    static var tableName: String { return "transformables" }

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
        return [
            "name": .text(name),
            "age": .integer(Int64(age)),
            "title": jobTitle == nil ? .null : .text(jobTitle!)
        ]
    }
}

extension Transformable {
    static var createTable: SQL {
        return """
        CREATE TABLE transformables (
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            title TEXT
        );
        """
    }

    static var fetchAll: SQL {
        return "SELECT * FROM transformables;"
    }

    static var fetchByName: SQL {
        return "SELECT * FROM transformables WHERE name=:name;"
    }

    static var insert: SQL {
        return "INSERT INTO transformables VALUES (:name, :age, :title);"
    }
}
