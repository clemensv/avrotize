namespace * com.example.orders

// Unix timestamp in milliseconds
typedef i64 Timestamp

enum OrderStatus {
  PENDING = 0,
  PAID = 10,
  SHIPPED = 20,
  DELIVERED = 30,
  CANCELLED = 40
}

struct Money {
  1: required string currency;
  2: required i64 amountMinor;
}

struct LineItem {
  1: required string sku;
  2: required string description;
  3: required i32 quantity;
  4: required Money unitPrice;
}

struct Address {
  1: required string street;
  2: required string city;
  3: required string postalCode;
  4: required string country;
  5: optional string region;
}

struct Order {
  1: required string orderId;
  2: required Timestamp createdAt;
  3: required OrderStatus status;
  4: required list<LineItem> items;
  5: required Money total;
  6: required Address shipTo;
  7: optional Address billTo;
  8: required map<string, string> metadata;
  9: optional string couponCode;
}
