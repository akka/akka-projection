
#create-table-h2
create table if not exists "akka_projection_offset_store" (
  "projection_name" VARCHAR(255) NOT NULL,
  "projection_key" VARCHAR(255) NOT NULL,
  "current_offset" VARCHAR(255) NOT NULL,
  "manifest" VARCHAR(4) NOT NULL,
  "mergeable" BOOLEAN NOT NULL,
  "last_updated" BIGINT NOT NULL,
  PRIMARY KEY("projection_name", "projection_key")
);

create index if not exists "projection_name_index" on "akka_projection_offset_store" ("projection_name");
#create-table-h2
