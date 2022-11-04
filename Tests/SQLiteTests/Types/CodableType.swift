import Foundation

struct CodableType: Codable, Equatable {
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

extension CodableType {
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
}

extension CodableType {
    static var createTable: String {
        """
        CREATE TABLE codable_types (
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
        """
        INSERT INTO codable_types VALUES (
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
        """
        INSERT OR REPLACE INTO codable_types VALUES (
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
        "SELECT * FROM codable_types ORDER BY id;"
    }

    static var getByID: String {
        "SELECT * FROM codable_types WHERE id=:id;"
    }
}
