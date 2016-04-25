
-- This is an example table create script for MySQL. It is not required for running the unit tests, which use H2.

create table visallo_vertex (
  id varchar(767) primary key,
  object longblob not null
);

create table visallo_edge (
  id varchar(767) primary key,
  in_vertex_id varchar(767),
  out_vertex_id varchar(767),
  object longblob not null,
  KEY idx_visallo_edge_in_vertex_id (in_vertex_id),
  KEY idx_visallo_edge_out_vertex_id (out_vertex_id)
);
create index idx_visallo_edge_in_vertex_id on visallo_edge (in_vertex_id);
create index idx_visallo_edge_out_vertex_id on visallo_edge (out_vertex_id);

create table visallo_metadata (
  id varchar(767) primary key,
  object longblob not null
);

create table visallo_streaming_properties (
  id varchar(767) primary key,
  data longblob not null,
  type varchar(4000) not null,
  length bigint not null
);
