database = "pgslam"
host = "localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261,localhost:26262,localhost:26263"
user = "root"

// As fast as you can!
//workers = 100
//rate = 1000000
workers = 3
rate = 100

prep = <<sql
create table t (
  id uuid not null primary key,
  data text
);
sql

exec = <<sql
insert into t (id, data) values ($1, $2);
sql

values {
  value {
    type = "random_uuid"
  }
  value {
    type = "text"
    string = "Some test text blah blah blah blah"
  }
}

nodes = 1

configs {
  config {
    nodes = 1
  }
  config {
    nodes = 3
  }
  config {
    nodes = 5
  }
}
