namespace * com.example.thrift
namespace py com.example.thrift.py

include "shared.thrift"
const string BUILD = "ignored"
typedef i64 UserId

enum Status {
  UNKNOWN = 0,
  ACTIVE = 10,
  SUSPENDED = 20
}

struct Address {
  1: required string street = "main";
  2: optional i32 zip;
}

struct User {
  1: required UserId id;
  2: optional string name;
  3: required bool enabled = true;
  4: required byte b;
  5: required i8 tiny;
  6: required i16 small;
  7: required i32 medium;
  8: required i64 large;
  9: required double ratio;
  10: required binary payload;
  11: required list<string> tags;
  12: required set<i32> scores;
  13: required map<string, Address> addresses;
  14: required map<i32, string> names_by_id;
  15: required Status status;
  16: optional Address home;
}

union SearchKey {
  1: string email;
  2: UserId id;
}

exception ServiceError {
  1: required string message;
  2: optional i32 code;
}

service UserService {
  User get(1: UserId id)
}
