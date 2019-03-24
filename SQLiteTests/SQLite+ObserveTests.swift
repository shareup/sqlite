import XCTest
@testable import SQLite

private struct Person: Codable, Hashable {
    var id: String
    var name: String
    var age: Int
    var title: String?
}

private struct Pet: Codable, Hashable {
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

private struct PetOwner: Equatable {
    var id: String
    var name: String
    var age: Int
    var title: String?
    var pet: Pet
}

class SQLiteObserveTests: XCTestCase {
    var database: SQLite.Database!
    var peopleObserver: AnyObject!
    var petsObserver: AnyObject!
    var petOwnersObserver: AnyObject!

    private var expectationAndResultsForPeople: (XCTestExpectation?, Array<Person>) = (nil, [])
    private lazy var onUpdatePeople: (Array<Person>) -> Void = { [unowned self] in
        return { [unowned self] (people: Array<Person>) -> Void in
            XCTAssertEqual(self.expectationAndResultsForPeople.1, people)
            self.expectationAndResultsForPeople.0?.fulfill()
        }
    }()

    private var expectationAndResultsForPets: (XCTestExpectation?, Array<Pet>) = (nil, [])
    private lazy var onUpdatePets: (Array<Pet>) -> Void = { [unowned self] in
        return { [unowned self] (pets: Array<Pet>) -> Void in
            XCTAssertEqual(self.expectationAndResultsForPets.1, pets)
            self.expectationAndResultsForPets.0?.fulfill()
        }
    }()

    private var expectationAndResultsForPetOwners: (XCTestExpectation?, Array<PetOwner>) = (nil, [])
    private lazy var onUpdatePetOwners: (Array<PetOwner>) -> Void = { [unowned self] in
        return { [unowned self] (petOwners: Array<PetOwner>) -> Void in
            XCTAssertEqual(self.expectationAndResultsForPetOwners.1, petOwners)
            self.expectationAndResultsForPetOwners.0?.fulfill()
        }
    }()

    override func setUp() {
        super.setUp()
        database = try! SQLite.Database(path: ":memory:")

        try! database.execute(raw: Person.createTable)
        try! database.execute(raw: Pet.createTable)
        let encoder = SQLite.Encoder(database)
        try! encoder.encode([_person1, _person2], using: Person.insert)
        try! encoder.encode([_pet1, _pet2], using: Pet.insert)
    }

    override func tearDown() {
        super.tearDown()
        peopleObserver = nil
        petsObserver = nil
        petOwnersObserver = nil
        database.close()
    }

    func testThrowsGivenInvalidSQL() {
        var token: AnyObject! = nil
        do {
            token = try database.observe("NOPE;", block: onUpdatePeople)
            XCTFail()
        } catch SQLite.Error.onPrepareStatement {
        } catch {
            XCTFail()
        }
        XCTAssertNil(token)
    }

    func testDelete() {
        _observeGetAllPeople()
        let expectation = self.expectation(description: "People observer notified")
        self.expectationAndResultsForPeople = (expectation, [_person2])
        try! database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
        waitForExpectations(timeout: 0.5)
    }

    func testInsert() {
        _observeGetAllPeople()
        let expectation = self.expectation(description: "People observer notified")
        let insertedPerson = Person(id: "3", name: "3", age: 3, title: "Limo Driver")
        self.expectationAndResultsForPeople = (expectation, [_person1, _person2, insertedPerson])
        try! database.write(Person.insert, arguments: insertedPerson.asArguments)
        waitForExpectations(timeout: 0.5)
    }

    func testUpdate() {
        _observeGetAllPeople()

        let expectation = self.expectation(description: "People observer notified")
        let replacedPerson = Person(id: "1", name: "1", age: 1, title: "Deep Sea Diver")
        var updatedPerson = _person2
        updatedPerson.title = "Technical Fellow"

        expectationAndResultsForPeople = (expectation, [updatedPerson, replacedPerson])

        let success = try! database.inTransaction {
            try! database.write(Person.insert, arguments: replacedPerson.asArguments)
            try! database.write(Person.updateTitleWithID, arguments: [
                "id": .text("2"), "title": .text("Technical Fellow")
            ])
        }
        XCTAssertTrue(success)

        waitForExpectations(timeout: 0.5)
    }

    func testSQLArgumentsAreRespectedAndMaintained() {
        let firstExpectation = self.expectation(description: "People observer notified with initial state")
        expectationAndResultsForPeople = (firstExpectation, [_person1])

        var token: AnyObject! = nil
        XCTAssertNoThrow(token = try database.observe(
            Person.getWithName,
            arguments: ["name": .text("Anthony")],
            block: onUpdatePeople)
        )
        peopleObserver = token

        waitForExpectations(timeout: 0.5)

        let insertedPerson = Person(id: "3", name: "Anthony", age: 99, title: nil)
        let secondExpectation = self.expectation(description: "People observer notified after insert")
        expectationAndResultsForPeople = (secondExpectation, [_person1, insertedPerson])
        XCTAssertNoThrow(try database.write(Person.insert, arguments: insertedPerson.asArguments))
        waitForExpectations(timeout: 0.5)

        let thirdExpectation = self.expectation(description: "People observer notified after delete")
        expectationAndResultsForPeople = (thirdExpectation, [insertedPerson])
        XCTAssertNoThrow(try database.write(Person.deleteWithID, arguments: ["id": .text("1")]))
        waitForExpectations(timeout: 0.5)
    }

    func testReceiveResultsOnlyForObservedType() {
        _observeGetAllPeople()

        let expectation = self.expectation(description: "People observer notified")
        
        let insertedPerson = Person(id: "3", name: "3", age: 3, title: "Limo Driver")
        let insertedPet = Pet(name: "Jumpy", ownerID: "3", type: "frog", registrationID: "3")
        let replacedPerson = Person(id: "1", name: "1", age: 1, title: "Deep Sea Diver")
        let replacedPet = Pet(name: "Slither", ownerID: "1", type: "snake", registrationID: "1")

        expectationAndResultsForPeople = (expectation, [insertedPerson, replacedPerson])

        let success = try! database.inTransaction {
            try! database.write(Person.deleteWithID, arguments: ["id": .text("2")])
            try! database.write(Person.insert, arguments: insertedPerson.asArguments)
            try! database.write(Person.insert, arguments: replacedPerson.asArguments)

            try! database.write(Pet.deleteWithName, arguments: ["name": .text("小飞球")])
            try! database.write(Pet.insert, arguments: insertedPet.asArguments)
            try! database.write(Pet.insert, arguments: replacedPet.asArguments)
        }
        XCTAssertTrue(success)

        waitForExpectations(timeout: 0.5)
    }

    func testReceiveResultsForAllObservedTypes() {
        _observeGetAllPeople()
        _observeGetAllPets()

        let peopleExpectation = self.expectation(description: "People observer notified")
        let petsExpectation = self.expectation(description: "Pets observer notified")

        let insertedPerson = Person(id: "3", name: "3", age: 3, title: "Limo Driver")
        let insertedPet = Pet(name: "Jumpy", ownerID: "3", type: "frog", registrationID: "3")
        let replacedPerson = Person(id: "1", name: "1", age: 1, title: "Deep Sea Diver")
        let replacedPet = Pet(name: "Slither", ownerID: "1", type: "snake", registrationID: "1")

        expectationAndResultsForPeople = (peopleExpectation, [insertedPerson, replacedPerson])
        expectationAndResultsForPets = (petsExpectation, [insertedPet, replacedPet])

        let success = try! database.inTransaction {
            try! database.write(Person.deleteWithID, arguments: ["id": .text("2")])
            try! database.write(Person.insert, arguments: insertedPerson.asArguments)
            try! database.write(Person.insert, arguments: replacedPerson.asArguments)

            try! database.write(Pet.deleteWithName, arguments: ["name": .text("小飞球")])
            try! database.write(Pet.insert, arguments: insertedPet.asArguments)
            try! database.write(Pet.insert, arguments: replacedPet.asArguments)
        }
        XCTAssertTrue(success)

        waitForExpectations(timeout: 0.5)
    }

    func testReceiveJoinedResultsAfterDeletion() {
        _observeGetAllPetOwners()

        let petOwnersExpectation = self.expectation(description: "Pet Owners observer notified")

        expectationAndResultsForPetOwners = (petOwnersExpectation, [_petOwner1])

        let success = try! database.inTransaction {
            try! database.write(Pet.deleteWithName, arguments: ["name": .text("小飞球")])
        }
        XCTAssertTrue(success)

        waitForExpectations(timeout: 0.5)
    }

    func testReceiveJoinedResultsAfterInsertion() {
        _observeGetAllPetOwners()

        let petOwnersExpectation = self.expectation(description: "Pet Owners observer notified")

        let insertedPerson = Person(id: "3", name: "Dog Lover", age: 29, title: nil)
        let insertedPet = Pet(name: "Rover", ownerID: "3", type: "dog", registrationID: "3")
        let petOwner3 = PetOwner(id: "3", name: "Dog Lover", age: 29, title: nil, pet: insertedPet)

        expectationAndResultsForPetOwners = (petOwnersExpectation, [_petOwner1, _petOwner2, petOwner3])

        let success = try! database.inTransaction {
            try! database.write(Person.insert, arguments: insertedPerson.asArguments)
            try! database.write(Pet.insert, arguments: insertedPet.asArguments)
        }
        XCTAssertTrue(success)

        waitForExpectations(timeout: 0.5)
    }

    func testReceiveJoinedResultsAfterUpdate() {
        _observeGetAllPetOwners()

        let petOwnersExpectation = self.expectation(description: "Pet Owners observer notified")

        var updatedPetOwner = _petOwner1
        var updatedPet = _pet1
        updatedPet.name = "February"
        updatedPetOwner.pet = updatedPet

        expectationAndResultsForPetOwners = (petOwnersExpectation, [updatedPetOwner, _petOwner2])

        let success = try! database.inTransaction {
            try! database.write(Pet.updateNameWithRegistrationID, arguments: [
                "name": .text("February"),
                "registration_id": .text("1"),
            ])
        }
        XCTAssertTrue(success)

        waitForExpectations(timeout: 0.5)
    }

    func testObserverIsNotRetained() {
        _observeGetAllPeople()

        peopleObserver = nil // deallocate observer

        let expectation = self.expectation(description: "Deallocated observer is not notified")
        expectationAndResultsForPeople = (expectation, []) // This should not be called

        XCTAssertNoThrow(try database.write(Person.deleteWithID, arguments: ["id": .text("1")]))

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) { expectation.fulfill() }

        waitForExpectations(timeout: 0.5)
    }
}

extension SQLiteObserveTests {
    private func _observeGetAllPeople() {
        let expectation = self.expectation(description: "People observer notified with initial state")
        expectationAndResultsForPeople = (expectation, [_person1, _person2])
        XCTAssertNoThrow(peopleObserver = try database.observe(Person.getAll, block: onUpdatePeople))
        waitForExpectations(timeout: 0.5)
    }

    private func _observeGetAllPets() {
        let expectation = self.expectation(description: "Pet observer notified with initial state")
        expectationAndResultsForPets = (expectation, [_pet1, _pet2])
        XCTAssertNoThrow(petsObserver = try database.observe(Pet.getAll, block: onUpdatePets))
        waitForExpectations(timeout: 0.5)
    }

    private func _observeGetAllPetOwners() {
        let expectation = self.expectation(description: "PetOwner observer notified with initial state")
        expectationAndResultsForPetOwners = (expectation, [_petOwner1, _petOwner2])
        XCTAssertNoThrow(petOwnersObserver = try database.observe(PetOwner.getAll, block: onUpdatePetOwners))
        waitForExpectations(timeout: 0.5)
    }
}

extension SQLiteObserveTests {
    private var _person1: Person {
        return Person(id: "1", name: "Anthony", age: 36, title: nil)
    }

    private var _person2: Person {
        return Person(id: "2", name: "Satya", age: 50, title: "CEO")
    }

    private var _pet1: Pet {
        return Pet(name: "Fido", ownerID: "1", type: "dog", registrationID: "1")
    }

    private var _pet2: Pet {
        return Pet(name: "小飞球", ownerID: "2", type: "cat", registrationID: "2")
    }

    private var _petOwner1: PetOwner {
        return PetOwner(id: "1", name: "Anthony", age: 36, title: nil, pet: _pet1)
    }

    private var _petOwner2: PetOwner {
        return PetOwner(id: "2", name: "Satya", age: 50, title: "CEO", pet: _pet2)
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
