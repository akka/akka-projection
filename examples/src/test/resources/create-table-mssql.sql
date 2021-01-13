
#create-table-mssql
IF NOT EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE OBJECT_ID = OBJECT_ID(N'akka_projection_offset_store') AND TYPE IN (N'U'))
BEGIN
CREATE TABLE akka_projection_offset_store (
  projection_name VARCHAR(255) NOT NULL,
  projection_key VARCHAR(255) NOT NULL,
  current_offset VARCHAR(255) NOT NULL,
  manifest VARCHAR(4) NOT NULL,
  mergeable BIT NOT NULL,
  last_updated BIGINT NOT NULL
)

ALTER TABLE akka_projection_offset_store ADD CONSTRAINT pk_projection_id PRIMARY KEY(projection_name, projection_key)

CREATE INDEX projection_name_index ON akka_projection_offset_store (projection_name)
END
#create-table-mssql
