import Combine
import CombineExtensions
import CombineTestExtensions
@testable import SQLite
import XCTest

final class SQLitePublisherTests: XCTestCase {
    var database: SQLiteDatabase!

    override func setUpWithError() throws {
        try super.setUpWithError()
        database = try SQLiteDatabase()

        try database.execute(raw: Person.createTable)
        try database.execute(raw: Pet.createTable)
        let encoder = SQLiteEncoder(database)
        try encoder.encode([_person1, _person2], using: Person.insert)
        try encoder.encode([_pet1, _pet2], using: Pet.insert)
    }

    override func tearDownWithError() throws {
        try super.tearDownWithError()
        try database.close()
    }

    func testReceivesCompletionWithErrorGivenInvalidSQL() throws {
        let ex = database
            .publisher("NOPE;")
            .expectFailure(
                { guard case .onPrepareStatement = $0 else { return XCTFail() } },
                failsOnOutput: true
            )
        wait(for: [ex], timeout: 0.5)
    }

    func testClosingAndReopeningDatabase() throws {
        try Sandbox.execute { directory in
            let path = directory.appendingPathComponent("test.db").path
            let db = try SQLiteDatabase(path: path)

            try db.write(Person.createTable)

            let ex = db
                .publisher(Person.self, Person.getAll, tables: ["people"])
                .removeDuplicates()
                .expectOutput(
                    [[], [_person1], [_person1, _person2]],
                    failsOnCompletion: true
                )

            try db.write(Person.insert, arguments: _person1.asArguments)

            try db.close()
            try db.reopen()

            try db.write(Person.insert, arguments: _person2.asArguments)

            wait(for: [ex], timeout: 2)

            try db.close()
        }
    }

    func testRetryingWhenDatabaseIsClosed() throws {
        let scheduler = DispatchQueue.main

        try Sandbox.execute { directory in
            let path = directory.appendingPathComponent("test.db").path

            let db = try SQLiteDatabase(path: path)
            try db.execute(raw: Person.createTable)
            let encoder = SQLiteEncoder(db)
            try encoder.encode([_person1, _person2], using: Person.insert)

            try db.close()

            let failureEx = expectation(description: "Should have failed twice")
            failureEx.expectedFulfillmentCount = 2
            failureEx.assertForOverFulfill = false // Just in case CI machines are slow

            let ex = db
                .readPublisher(Person.getAll)
                .retryIf(
                    { error in
                        if error.isClosed {
                            failureEx.fulfill()
                            return true
                        } else {
                            return false
                        }
                    },
                    after: .milliseconds(50),
                    scheduler: scheduler
                )
                .expectOutput(
                    [_person1, _person2],
                    expectToFinish: true
                )

            wait(for: [failureEx], timeout: 2)
            try db.reopen()

            wait(for: [ex], timeout: 2)

            try db.close()
        }
    }

    func testCancellingForeverCancelsSubscriptions() throws {
        let expected = [_person1, _person2]

        let ex = expectation(description: "Should have received initial values")
        let sub = database
            .publisher(Person.self, Person.getAll)
            .sink(
                receiveCompletion: { completion in XCTFail(String(describing: completion)) },
                receiveValue: { XCTAssertEqual(expected, $0); ex.fulfill() }
            )

        wait(for: [ex], timeout: 0.5)

        sub.cancel()
        try database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])

        RunLoop.current.run(until: Date(timeIntervalSinceNow: 0.1))
    }

    func testReceivesCurrentValuesWhenSubscribing() throws {
        let ex = database
            .publisher(Person.getAll)
            .expectOutput([[_person1.asArguments, _person2.asArguments]])
        wait(for: [ex], timeout: 0.5)
    }

    func testDeleteAsSQLiteRow() throws {
        let expected: [[SQLiteRow]] = [
            [_person1.asArguments, _person2.asArguments],
            [_person2.asArguments],
        ]

        let ex = database
            .publisher(Person.getAll, tables: ["people"])
            .expectOutput(expected)

        try database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
        wait(for: [ex], timeout: 0.5)
    }

    func testDelete() throws {
        let expected: [[Person]] = [
            [_person1, _person2],
            [_person2],
        ]

        let ex = database
            .publisher(Person.getAll, tables: ["people"])
            .expectOutput(expected)

        try database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
        wait(for: [ex], timeout: 0.5)
    }

    func testDeleteFirstWhere() throws {
        let ex = database
            .publisher(Person.getAll, tables: ["people"])
            .filter { $0.count == 1 }
            .expectOutput([[_person2]])

        try database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
        wait(for: [ex], timeout: 0.5)
    }

    func testDeleteMappedToName() throws {
        let expected: [[String]] = [
            [_person1.name, _person2.name],
            [_person2.name],
        ]

        let ex = database
            .publisher(Person.self, Person.getAll, tables: ["people"])
            .map { $0.map(\.name) }
            .expectOutput(expected)

        try database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
        wait(for: [ex], timeout: 0.5)
    }

    func testDeleteAll() throws {
        let expected: [[Person]] = [
            [_person1, _person2],
            [],
        ]

        let ex = database
            .publisher(Person.self, Person.getAll)
            .expectOutput(expected)

        try database.execute(raw: "DELETE FROM people;")
        wait(for: [ex], timeout: 0.5)
    }

    func testInsert() throws {
        let person3 = Person(id: "3", name: "New Human", age: 1, title: "newborn")
        let pet3 = Pet(
            name: "Camo the Camel",
            ownerID: person3.id,
            type: "camel",
            registrationID: "3"
        )
        let petOwner3 = PetOwner(
            id: person3.id, name: person3.name, age: person3.age, title: person3.title,
            pet: pet3
        )

        let expected: [[PetOwner]] = [
            [_petOwner1, _petOwner2],
            [_petOwner1, _petOwner2, petOwner3], // After insert of pet
        ]

        let ex = database
            .publisher(PetOwner.self, PetOwner.getAll, tables: ["people", "pets"])
            .removeDuplicates()
            .expectOutput(expected, failsOnCompletion: true)

        try database.inTransaction { db in
            try db.write(Person.insert, arguments: person3.asArguments)
            try db.write(Pet.insert, arguments: pet3.asArguments)
        }

        wait(for: [ex], timeout: 0.5)
    }

    func testOnlyPublishesWhenDataHasChanged() throws {
        let person3 = Person(id: "3", name: "New Human", age: 1, title: "newborn")
        let pet3 = Pet(
            name: "Camo the Camel",
            ownerID: person3.id,
            type: "camel",
            registrationID: "3"
        )
        let petOwner3 = PetOwner(
            id: person3.id, name: person3.name, age: person3.age, title: person3.title,
            pet: pet3
        )

        let expected: [[PetOwner]] = [
            [_petOwner1, _petOwner2],
            [_petOwner1, _petOwner2, petOwner3], // After insert of pet,
        ]

        var publishCount = 0

        let ex = database
            .publisher(PetOwner.self, PetOwner.getAll, tables: ["people", "pets"])
            .handleEvents(receiveOutput: { _ in publishCount += 1 })
            .expectOutput(expected, failsOnCompletion: true)

        try database.inTransaction { db in
            try db.write(Person.insert, arguments: person3.asArguments)
            try db.write(Pet.insert, arguments: pet3.asArguments)
        }

        wait(for: [ex], timeout: 2)

        XCTAssertEqual(2, publishCount)
    }

    func testPublishesWhenDataHasChangedInObservedTable() throws {
        var changedPetOwner2 = _petOwner2
        var changedPet = changedPetOwner2.pet
        changedPet.name = "NEW NAME"
        changedPetOwner2.pet = changedPet

        let expected: [[PetOwner]] = [
            [_petOwner1, _petOwner2],
            [_petOwner1, changedPetOwner2],
        ]

        var publishCount = 0

        let ex = database
            .publisher(PetOwner.self, PetOwner.getAll, tables: ["pets"])
            .handleEvents(receiveOutput: { _ in publishCount += 1 })
            .expectOutput(expected, failsOnCompletion: true)

        try database.write(
            Pet.updateNameWithRegistrationID,
            arguments: ["name": "NEW NAME".sqliteValue, "registration_id": "2".sqliteValue]
        )

        wait(for: [ex], timeout: 2)

        XCTAssertEqual(2, publishCount)
    }

    func testDoesNotPublishWhenDataHasChangedInUnobservedTable() throws {
        var changedPetOwner2 = _petOwner2
        var changedPet = changedPetOwner2.pet
        changedPet.name = "NEW NAME"
        changedPetOwner2.pet = changedPet

        let expected: [[PetOwner]] = [
            [_petOwner1, _petOwner2],
            [_petOwner1, changedPetOwner2], // This should not be received.
        ]

        var publishCount = 0

        let ex = database
            .publisher(PetOwner.self, PetOwner.getAll, tables: ["people"])
            .handleEvents(receiveOutput: { _ in publishCount += 1 })
            .expectOutput(expected, failsOnCompletion: true)
        ex.isInverted = true

        try database.write(
            Pet.updateNameWithRegistrationID,
            arguments: ["name": "NEW NAME".sqliteValue, "registration_id": "2".sqliteValue]
        )

        wait(for: [ex], timeout: 0.1)

        XCTAssertEqual(1, publishCount)
    }

    func testPublishesForAllTablesWhenEmptyArrayIsPassed() throws {
        var changedPetOwner1 = _petOwner1
        changedPetOwner1.title = "NEW TITLE"

        var changedPetOwner2 = _petOwner2
        var changedPet = changedPetOwner2.pet
        changedPet.name = "NEW NAME"
        changedPetOwner2.pet = changedPet

        let expected: [[PetOwner]] = [
            [_petOwner1, _petOwner2],
            [changedPetOwner1, _petOwner2],
            [changedPetOwner1, changedPetOwner2],
        ]

        var publishCount = 0

        let ex = database
            .publisher(PetOwner.self, PetOwner.getAll)
            .handleEvents(receiveOutput: { _ in publishCount += 1 })
            .expectOutput(expected, failsOnCompletion: true)

        try database.write(
            Person.updateTitleWithID,
            arguments: ["id": changedPetOwner1.id.sqliteValue, "title": "NEW TITLE".sqliteValue]
        )

        try database.write(
            Pet.updateNameWithRegistrationID,
            arguments: ["name": "NEW NAME".sqliteValue, "registration_id": "2".sqliteValue]
        )

        wait(for: [ex], timeout: 2)

        XCTAssertEqual(3, publishCount)
    }
}

extension SQLitePublisherTests {
    private var _person1: Person {
        Person(id: "1", name: "Anthony", age: 36, title: nil)
    }

    private var _person2: Person {
        Person(id: "2", name: "Satya", age: 50, title: "CEO")
    }

    private var _pet1: Pet {
        Pet(name: "Fido", ownerID: "1", type: "dog", registrationID: "1")
    }

    private var _pet2: Pet {
        Pet(name: "小飞球", ownerID: "2", type: "cat", registrationID: "2")
    }

    private var _petOwner1: PetOwner {
        PetOwner(id: "1", name: "Anthony", age: 36, title: nil, pet: _pet1)
    }

    private var _petOwner2: PetOwner {
        PetOwner(id: "2", name: "Satya", age: 50, title: "CEO", pet: _pet2)
    }
}
