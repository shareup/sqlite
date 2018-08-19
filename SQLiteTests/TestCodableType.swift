import Foundation

struct TestCodableType: Codable, Equatable {
    var id: Int
    var uuid: UUID
    var string: String
    var data: Data
    var url: URL
    var optionalString: String?
    var optionalDate: Date?
    var inner: Inner

    private enum CodingKeys: String, CodingKey {
        case id
        case uuid
        case string
        case data
        case url
        case optionalString = "optional_string"
        case optionalDate = "optional_date"
        case inner
    }
}

struct Inner: Codable, Equatable {
    var string: String
    var data: Data
    var date: Date
    var optionalBool: Bool?

    private enum CodingKeys: String, CodingKey {
        case string
        case data
        case date
        case optionalBool = "optional_bool"
    }
}

extension TestCodableType {
    static var createTable: String {
        return """
        CREATE TABLE test_codable_types (
            id INTEGER UNIQUE NOT NULL,
            uuid TEXT NOT NULL,
            string TEXT NOT NULL,
            data BLOB NOT NULL,
            url TEXT NOT NULL,
            optional_string TEXT,
            optional_date TEXT,
            inner BLOB NOT NULL
        );
        """
    }

    static var insert: String {
        return """
        INSERT INTO test_codable_types VALUES (
            :id,
            :uuid,
            :string,
            :data,
            :url,
            :optional_string,
            :optional_date,
            :inner
        );
        """
    }

    static var upsert: String {
        return """
        INSERT OR REPLACE INTO test_codable_types VALUES (
            :id,
            :uuid,
            :string,
            :data,
            :url,
            :optional_string,
            :optional_date,
            :inner
        );
        """
    }

    static var getAll: String {
        return "SELECT * FROM test_codable_types ORDER BY id;"
    }

    static var getByID: String {
        return "SELECT * FROM test_codable_types WHERE id=:id;"
    }
}
