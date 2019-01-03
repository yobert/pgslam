database = "pgslam"
host = "localhost:26257,localhost:26258,localhost:26259,localhost:26260,localhost:26261,localhost:26262,localhost:26263"
user = "root"

nodes = 1
rate = 1000000

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

