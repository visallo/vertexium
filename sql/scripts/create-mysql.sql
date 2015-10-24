
-- This is an example table create script for MySQL. It is not required for running the unit tests, which use H2.

create table vertexium_vertex (
  id varchar(255) primary key,
  object longtext not null
);

create table vertexium_edge (
  id varchar(255) primary key,
  in_vertex_id varchar(255),
  out_vertex_id varchar(255),
  object longtext not null
);

create table vertexium_metadata (
  id varchar(255) primary key,
  object longtext not null
);

create table vertexium_streaming_properties (
  id varchar(255) primary key,
  data longblob not null,
  type varchar(255) not null,
  length bigint not null
);
