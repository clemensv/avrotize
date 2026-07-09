$version: "2.0"
namespace com.example.catalog

@documentation("A sellable product in the catalog")
structure Product {
    @required
    sku: String
    @required
    name: String
    description: String
    price: Money
    category: Category
    availability: Availability
    tags: Tags
    attributes: Attributes
}

@documentation("A monetary amount in minor units")
structure Money {
    @required
    currency: String
    @required
    amountMinor: Long
}

enum Category {
    ELECTRONICS
    APPAREL
    HOME
    GROCERY
}

intEnum Availability {
    IN_STOCK = 0
    BACKORDER = 1
    DISCONTINUED = 9
}

list Tags {
    member: String
}

map Attributes {
    key: String
    value: String
}
