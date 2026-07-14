$version: "2.0"
namespace com.example

@documentation("Postal address")
structure Address {
    @required
    street: String
    city: String
}

@documentation("Person data")
structure Person {
    @required
    name: String
    age: Integer
    address: Address
    tags: Tags
    favorite: Color
    createdAt: Timestamp
}

enum Color {
    RED
    GREEN
    BLUE
}

intEnum Status {
    OK = 0
    ERROR = 9
}

list Names {
    member: String
}

map Tags {
    key: String
    value: String
}

union Payload {
    text: String
    count: Integer
    address: Address
}

service ExampleService {
    version: "2026-06-19"
    operations: [GetPerson]
}

operation GetPerson {
    input: Person
    output: Person
}
