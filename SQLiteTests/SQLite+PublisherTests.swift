import XCTest
import Combine
@testable import SQLite

class SQLitePublisherTests: XCTestCase {
    var database: SQLite.Database!

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
        database.close()
    }

    func testReceivesCompletionWithErrorGivenInvalidSQL() {
        let expectation = self.expectation(description: "Completes with error")
        let publisher = database.publisher("NOPE;")
        let receiveCompletion: (Subscribers.Completion<Error>) -> Void = { completion in
            switch completion {
            case .finished:
                XCTFail("Should have completed with error")
            case .failure(let error):
                guard case SQLite.Error.onPrepareStatement = error else {
                    return XCTFail("Incorrect error: \(error)")
                }
            }
            expectation.fulfill()
        }
        let receiveValue: (Array<SQLiteRow>) -> Void = { rows in
            XCTFail("Should have completed with error, not received \(rows)")
            expectation.fulfill()
        }
        let subscriber = publisher.sink(receiveCompletion: receiveCompletion, receiveValue: receiveValue)
        waitForExpectations(timeout: 0.5)
        subscriber.cancel()
    }

    func testCancellingSinkCancelsSubscriptions() {
        let expectation = self.expectation(description: "Timeout called")

        var expected: Array<Array<Person>> = [[_person1, _person2]]

        let receiveCompletion: (Subscribers.Completion<Error>) -> Void = { completion in
            XCTFail("Should not receive completion: \(String(describing: completion))")
        }

        let receiveValue: (Array<Person>) -> Void = { people in
            let first = expected.removeFirst()
            XCTAssertEqual(first, people)
        }

        let publisher: AnyPublisher<Array<Person>, Error> = database.publisher(Person.getAll)
        let sink = publisher.sink(receiveCompletion: receiveCompletion, receiveValue: receiveValue)
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
            sink.cancel()
            try! self.database.write(Person.deleteWithID, arguments: ["id": .text(self._person1.id)])
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) { expectation.fulfill() }
        }
        waitForExpectations(timeout: 0.5)
    }

    func testDeleteAsSQLiteRow() {
        let expectation = self.expectation(description: "Received two notifications")

        var expected: Array<Array<SQLiteRow>> = [
            [_person1.asArguments, _person2.asArguments],
            [_person2.asArguments],
        ]

        let receiveCompletion: (Subscribers.Completion<Error>) -> Void = { completion in
            XCTFail("Should not receive completion: \(String(describing: completion))")
        }

        let receiveValue: (Array<SQLiteRow>) -> Void = { rows in
            let first = expected.removeFirst()
            XCTAssertEqual(first, rows)
            if expected.isEmpty {
                expectation.fulfill()
            }
        }

        let publisher: AnyPublisher<Array<SQLiteRow>, Error> = database.publisher(Person.getAll)
        let sink = publisher.sink(receiveCompletion: receiveCompletion, receiveValue: receiveValue)
        try! database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
        waitForExpectations(timeout: 0.5)
        sink.cancel()
    }

    func testDelete() {
        let expectation = self.expectation(description: "Received two notifications")

        var expected: Array<Array<Person>> = [
            [_person1, _person2],
            [_person2],
        ]

        let receiveCompletion: (Subscribers.Completion<Error>) -> Void = { completion in
            XCTFail("Should not receive completion: \(String(describing: completion))")
        }

        let receiveValue: (Array<Person>) -> Void = { people in
            let first = expected.removeFirst()
            XCTAssertEqual(first, people)
            if expected.isEmpty {
                expectation.fulfill()
            }
        }

        let publisher: AnyPublisher<Array<Person>, Error> = database.publisher(Person.getAll)
        let sink = publisher.sink(receiveCompletion: receiveCompletion, receiveValue: receiveValue)
        try! database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
        waitForExpectations(timeout: 0.5)
        sink.cancel()
    }

//    func testDelete() {
//        _observeGetAllPeople()
//        let expectation = self.expectation(description: "People observer notified")
//        self.expectationAndResultsForPeople = (expectation, [_person2])
//        try! database.write(Person.deleteWithID, arguments: ["id": .text(_person1.id)])
//        waitForExpectations(timeout: 0.5)
//    }
//
//    func testInsert() {
//        _observeGetAllPeople()
//        let expectation = self.expectation(description: "People observer notified")
//        let insertedPerson = Person(id: "3", name: "3", age: 3, title: "Limo Driver")
//        self.expectationAndResultsForPeople = (expectation, [_person1, _person2, insertedPerson])
//        try! database.write(Person.insert, arguments: insertedPerson.asArguments)
//        waitForExpectations(timeout: 0.5)
//    }
}

extension SQLitePublisherTests {
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
