import Foundation
import SQLite

struct PetOwner: Equatable {
    var id: String
    var name: String
    var age: Int
    var title: String?
    var pet: Pet
}

struct Person: Codable, Hashable {
    var id: String
    var name: String
    var age: Int
    var title: String?
}

struct Pet: Codable, Hashable {
    var name: String
    var ownerID: String
    var type: String
    var registrationID: String

    enum CodingKeys: String, CodingKey {
        case name
        case ownerID = "owner_id"
        case type
        case registrationID = "registration_id"
    }
}

extension PetOwner: SQLiteTransformable {
    init(row: SQLiteRow) throws {
        self.id = try row.value(for: "person_id")
        self.name = try row.value(for: "person_name")
        self.age = try row.value(for: "person_age")
        self.title = row.optionalValue(for: "person_title")
        self.pet = Pet(
            name: try row.value(for: "pet_name"),
            ownerID: try row.value(for: "pet_owner_id"),
            type: try row.value(for: "pet_type"),
            registrationID: try row.value(for: "pet_registration_id")
        )
    }

    static var getAll: SQL {
        return """
        SELECT
            people.id AS person_id,
            people.name AS person_name,
            people.age AS person_age,
            people.title AS person_title,
            pets.name AS pet_name,
            pets.owner_id AS pet_owner_id,
            pets.type AS pet_type,
            pets.registration_id AS pet_registration_id
        FROM people INNER JOIN pets ON pets.owner_id = people.id;
        """
    }
}

extension Person: SQLiteTransformable {
    init(row: SQLiteRow) throws {
        self.id = try row.value(for: CodingKeys.id)
        self.name = try row.value(for: CodingKeys.name)
        self.age = try row.value(for: CodingKeys.age)
        self.title = row.optionalValue(for: CodingKeys.title)
    }

    var asArguments: SQLiteArguments {
        let titleValue: SQLite.Value
        if let title = self.title {
            titleValue = .text(title)
        } else {
            titleValue = .null
        }

        return [
            "id": .text(self.id),
            "name": .text(self.name),
            "age": .integer(Int64(self.age)),
            "title": titleValue
        ]
    }

    static var createTable: SQL {
        return """
        CREATE TABLE people (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            title TEXT
        );
        """
    }

    static var getAll: SQL {
        return "SELECT * FROM people;"
    }

    static var getWithID: SQL {
        return "SELECT * FROM people WHERE id=:id;"
    }

    static var getWithName: SQL {
        return "SELECT * FROM people where name=:name;"
    }

    static var insert: SQL {
        return "INSERT OR REPLACE INTO people VALUES (:id, :name, :age, :title);"
    }

    static var updateTitleWithID: SQL {
        return "UPDATE people SET title=:title WHERE id=:id;"
    }

    static var deleteWithID: SQL {
        return "DELETE FROM people WHERE id=:id;"
    }
}

extension Pet: SQLiteTransformable {
    init(row: SQLiteRow) throws {
        self.name = try row.value(for: CodingKeys.name)
        self.ownerID = try row.value(for: CodingKeys.ownerID)
        self.type = try row.value(for: CodingKeys.type)
        self.registrationID = try row.value(for: CodingKeys.registrationID)
    }

    var asArguments: SQLiteArguments {
        return [
            "name": .text(self.name),
            "owner_id": .text(self.ownerID),
            "type": .text(self.type),
            "registration_id": .text(self.registrationID)
        ]
    }

    static var createTable: SQL {
        return """
        CREATE TABLE pets (
            name TEXT NOT NULL,
            owner_id TEXT NOT NULL UNIQUE,
            type TEXT NOT NULL,
            registration_id TEXT NOT NULL UNIQUE,
            PRIMARY KEY (name, owner_id)
        );
        """
    }

    static var getAll: SQL {
        return "SELECT * FROM pets;"
    }

    static var getWithName: SQL {
        return "SELECT * FROM pets WHERE name=:name;"
    }

    static var getWithOwnerID: SQL {
        return "SELECT * FROM pets WHERE owner_id=:owner_id;"
    }

    static var insert: SQL {
        return "INSERT OR REPLACE INTO pets VALUES (:name, :owner_id, :type, :registration_id);"
    }

    static var updateNameWithRegistrationID: SQL {
        return "UPDATE pets SET name=:name WHERE registration_id=:registration_id;"
    }

    static var deleteWithName: SQL {
        return "DELETE FROM pets WHERE name=:name;"
    }
}
