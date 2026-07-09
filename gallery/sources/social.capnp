@0xc3f9a1b2d4e5f607;

enum Visibility {
  everyone @0;
  followers @1;
  private @2;
}

struct User {
  id @0 :Text;
  handle @1 :Text;
  displayName @2 :Text;
  followerCount @3 :UInt32;
  verified @4 :Bool;
}

struct Comment {
  author @0 :User;
  text @1 :Text;
  createdAt @2 :Int64;
}

struct Post {
  id @0 :Text;
  author @1 :User;
  body @2 :Text;
  createdAt @3 :Int64;
  visibility @4 :Visibility;
  tags @5 :List(Text);
  likeCount @6 :UInt32;
  comments @7 :List(Comment);
}
