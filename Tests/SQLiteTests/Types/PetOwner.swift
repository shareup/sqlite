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
        id = try row.value(for: "person_id")
        name = try row.value(for: "person_name")
        age = try row.value(for: "person_age")
        title = row.optionalValue(for: "person_title")
        pet = Pet(
            name: try row.value(for: "pet_name"),
            ownerID: try row.value(for: "pet_owner_id"),
            type: try row.value(for: "pet_type"),
            registrationID: try row.value(for: "pet_registration_id")
        )
    }

    static var getAll: SQL {
        """
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
        id = try row.value(for: CodingKeys.id)
        name = try row.value(for: CodingKeys.name)
        age = try row.value(for: CodingKeys.age)
        title = row.optionalValue(for: CodingKeys.title)
    }

    var asArguments: SQLiteArguments {
        let titleValue: SQLiteValue
        if let title {
            titleValue = .text(title)
        } else {
            titleValue = .null
        }

        return [
            "id": .text(id),
            "name": .text(name),
            "age": .integer(Int64(age)),
            "title": titleValue,
        ]
    }

    static var createTable: SQL {
        """
        CREATE TABLE people (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            title TEXT
        );
        """
    }

    static var getAll: SQL {
        "SELECT * FROM people;"
    }

    static var getWithID: SQL {
        "SELECT * FROM people WHERE id=:id;"
    }

    static var getWithName: SQL {
        "SELECT * FROM people where name=:name;"
    }

    static var insert: SQL {
        "INSERT OR REPLACE INTO people VALUES (:id, :name, :age, :title);"
    }

    static var updateTitleWithID: SQL {
        "UPDATE people SET title=:title WHERE id=:id;"
    }

    static var deleteWithID: SQL {
        "DELETE FROM people WHERE id=:id;"
    }
}

extension Pet: SQLiteTransformable {
    init(row: SQLiteRow) throws {
        name = try row.value(for: CodingKeys.name)
        ownerID = try row.value(for: CodingKeys.ownerID)
        type = try row.value(for: CodingKeys.type)
        registrationID = try row.value(for: CodingKeys.registrationID)
    }

    var asArguments: SQLiteArguments {
        [
            "name": .text(name),
            "owner_id": .text(ownerID),
            "type": .text(type),
            "registration_id": .text(registrationID),
        ]
    }

    static var createTable: SQL {
        """
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
        "SELECT * FROM pets;"
    }

    static var getWithName: SQL {
        "SELECT * FROM pets WHERE name=:name;"
    }

    static var getWithOwnerID: SQL {
        "SELECT * FROM pets WHERE owner_id=:owner_id;"
    }

    static var insert: SQL {
        "INSERT OR REPLACE INTO pets VALUES (:name, :owner_id, :type, :registration_id);"
    }

    static var updateNameWithRegistrationID: SQL {
        "UPDATE pets SET name=:name WHERE registration_id=:registration_id;"
    }

    static var deleteWithName: SQL {
        "DELETE FROM pets WHERE name=:name;"
    }
}
