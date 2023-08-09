import Combine
import CombineExtensions
import CombineTestExtensions
@testable import SQLite
import SQLite3
import Synchronized
import XCTest

final class SQLitePublisherTests: XCTestCase {
    var database: SQLiteDatabase!

    override func setUpWithError() throws {
        try super.setUpWithError()
        database = try SQLiteDatabase()

        try database.execute(raw: Person.createTable)
        try database.execute(raw: Pet.createTable)

        try [_person1, _person2].forEach { person in
            try database.write(Person.insert, arguments: person.asArguments)
        }

        try [_pet1, _pet2].forEach { pet in
            try database.write(Pet.insert, arguments: pet.asArguments)
        }
    }

    override func tearDownWithError() throws {
        try super.tearDownWithError()
        database = nil
    }

    func testReceivesCompletionWithErrorGivenInvalidSQL() throws {
        let ex = database
            .publisher("NOPE;")
            .expectFailure(
                { error in
                    guard case .SQLITE_ERROR = error else {
                        return XCTFail("'\(error)' should be SQLITE_ERROR")
                    }
                },
                failsOnOutput: true
            )
        wait(for: [ex], timeout: 0.5)
    }

    func testSuspendingAndResumingDatabase() throws {
        try Sandbox.execute { directory in
            let path = directory.appendingPathComponent("test.db").path
            let db = try SQLiteDatabase(path: path)
            defer { try? db.close() }

            try db.write(Person.createTable)

            let expectedOutput = Locked([
                (0, []),
                (1, [_person1]),
                (2, [_person1, _person2]),
            ])
            let ex = db
                .publisher(Person.self, Person.getAll)
                .removeDuplicates()
                .expectOutput({ [_person1, _person2] people in
                    let (i, expected) = expectedOutput.access { $0.removeFirst() }
                    XCTAssertEqual(expected, people)

                    switch i {
                    case 0:
                        try db.write(
                            Person.insert,
                            arguments: _person1.asArguments
                        )

                        db.suspend()
                        db.resume()

                        return .moreExpected

                    case 1:
                        try db.write(
                            Person.insert,
                            arguments: _person2.asArguments
                        )
                        return .moreExpected

                    case 2:
                        return .finished

                    default:
                        XCTFail()
                        return .finished
                    }
                }, failsOnCompletion: true)

            wait(for: [ex], timeout: 2)
        }
    }

    func testPublishersPublishWhenDatabaseIsSuspended() throws {
        try Sandbox.execute { directory in
            let path = directory.appendingPathComponent("test.db").path

            let db = try SQLiteDatabase(path: path)
            defer { try? db.close() }
            try db.execute(raw: Person.createTable)

            try [_person1, _person2].forEach { person in
                try db.write(
                    Person.insert,
                    arguments: person.asArguments
                )
            }

            db.suspend()

            let ex = db
                .readPublisher(Person.getAll)
                .expectOutput(
                    [_person1, _person2],
                    expectToFinish: true
                )

            wait(for: [ex], timeout: 2)
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
            .publisher(Person.getAll)
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
            .publisher(Person.getAll)
            .expectOutput(expected)

        try database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
        wait(for: [ex], timeout: 0.5)
    }

    func testDeleteFirstWhere() throws {
        let ex = database
            .publisher(Person.getAll)
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
            .publisher(Person.self, Person.getAll)
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
            .publisher(PetOwner.self, PetOwner.getAll)
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
            .publisher(PetOwner.self, PetOwner.getAll)
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

        let expectedOutput = Locked([
            [_petOwner1, _petOwner2],
            [_petOwner1, changedPetOwner2],
        ])

        var publishCount = 0

        let ex = database
            .publisher(PetOwner.self, PetOwner.getAll)
            .handleEvents(receiveOutput: { _ in publishCount += 1 })
            .expectOutput({ [db = database!] petOwners in
                let expected = expectedOutput.access { $0.removeFirst() }
                XCTAssertEqual(expected, petOwners)
                switch publishCount {
                case 1:
                    try db.write(
                        Pet.updateNameWithRegistrationID,
                        arguments: [
                            "name": "NEW NAME".sqliteValue,
                            "registration_id": "2".sqliteValue,
                        ]
                    )
                    return .moreExpected

                case 2:
                    return .finished

                default:
                    XCTFail()
                    return .finished
                }
            }, failsOnCompletion: true)

        wait(for: [ex], timeout: 2)

        XCTAssertEqual(2, publishCount)
    }

    func testDoesNotPublishWhenDataHasChangedInUnobservedTable() throws {
        var changedPet = _pet2
        changedPet.name = "NEW NAME"

        let expected: [[Person]] = [
            [_person1, _person2],
            [_person1, _person2], // This should not be received.
        ]

        var publishCount = 0

        let ex = database
            .publisher(Person.self, Person.getAll)
            .handleEvents(receiveOutput: { _ in publishCount += 1 })
            .expectOutput(expected, failsOnCompletion: true)
        ex.isInverted = true

        try database.write(
            Pet.updateNameWithRegistrationID,
            arguments: [
                "name": changedPet.name.sqliteValue,
                "registration_id": changedPet.registrationID.sqliteValue,
            ]
        )

        wait(for: [ex], timeout: 0.1)

        XCTAssertEqual(1, publishCount)
    }

    func testTouchPublishesAllTables() throws {
        let peopleEx = database
            .publisher(Person.self, Person.getAll)
            .expectOutput([
                [_person1, _person2],
                [_person1, _person2],
            ], failsOnCompletion: true)

        let petsEx = database
            .publisher(Pet.self, Pet.getAll)
            .expectOutput([
                [_pet1, _pet2],
                [_pet1, _pet2],
            ], failsOnCompletion: true)

        database.touch()

        wait(for: [peopleEx, petsEx], timeout: 2)
    }

    // This is a regression test. In an early version of
    // `SQLiteDatabase.publisher`, creating a publisher outside of
    // a `Task`, and then async-iterating over it inside of the
    // `Task` would cause a race condition in which it was possible
    // that changes occurring after the publisher was created but
    // before the publisher's internal observation was created, would
    // never be published.
    func testPublishersCanBeUsedOnDifferentThreads() async throws {
        let peoplePub = database.publisher(Person.self, Person.getAll)

        let task = Task<[Person], Error> {
            for try await people in peoplePub.values {
                if people.count == 3 {
                    return people
                }
            }
            XCTFail()
            return []
        }

        let cancellation = Task {
            try await Task.sleep(nanoseconds: NSEC_PER_MSEC * 50)
            task.cancel()
        }

        let person3 = Person(id: "3", name: "Wonderbread", age: 76, title: nil)
        try await database.write(Person.insert, arguments: person3.asArguments)

        let people = try await task.value
        cancellation.cancel()
        XCTAssertEqual(
            [_person1, _person2, person3],
            people
        )
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
