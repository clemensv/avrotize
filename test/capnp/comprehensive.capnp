@0xbf5147a1d903c2f0;

struct PrimitiveSample {
  voidField @0 :Void;
  boolField @1 :Bool;
  int8Field @2 :Int8;
  int16Field @3 :Int16;
  int32Field @4 :Int32;
  int64Field @5 :Int64;
  uint8Field @6 :UInt8;
  uint16Field @7 :UInt16;
  uint32Field @8 :UInt32;
  uint64Field @9 :UInt64;
  float32Field @10 :Float32;
  float64Field @11 :Float64;
  textField @12 :Text;
  dataField @13 :Data;
}

struct Address {
  street @0 :Text;
  zip @1 :UInt32;
}

enum Color {
  red @0;
  green @1;
  blue @2;
}

struct Person {
  name @0 :Text;
  age @1 :UInt16;
  address @2 :Address;
  tags @3 :List(Text);
  scores @4 :List(Int32);
  favorite @5 :Color;
  contact @6 :group {
    email @0 :Text;
    phone @1 :Text;
  }
  union {
    noPet @7 :Void;
    dogName @8 :Text;
    catName @9 :Text;
  }
  struct Preference {
    label @0 :Text;
    weight @1 :Float64;
  }
  prefs @10 :List(Preference);
  enum Status {
    active @0;
    inactive @1;
  }
  status @11 :Status;
}

interface IgnoredService {
  ping @0 () -> ();
}
const ignored :Text = "skip";
annotation ignoredAnn(field) :Text;
