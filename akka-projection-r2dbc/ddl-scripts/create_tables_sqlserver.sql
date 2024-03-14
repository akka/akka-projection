IF object_id('event_journal') is null
  CREATE TABLE event_journal(
    slice INT NOT NULL,
    entity_type NVARCHAR(255) NOT NULL,
    persistence_id NVARCHAR(255) NOT NULL,
    seq_nr NUMERIC(10,0) NOT NULL,
    db_timestamp datetime2(6) NOT NULL,
    event_ser_id INTEGER NOT NULL,
    event_ser_manifest NVARCHAR(255) NOT NULL,
    event_payload VARBINARY(MAX) NOT NULL,
    deleted BIT DEFAULT 0 NOT NULL,
    writer NVARCHAR(255) NOT NULL,
    adapter_manifest NVARCHAR(255) NOT NULL,
    tags NVARCHAR(255),

    meta_ser_id INTEGER,
    meta_ser_manifest NVARCHAR(255),
    meta_payload VARBINARY(MAX),
    PRIMARY KEY(persistence_id, seq_nr)
  );

IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'event_journal_slice_idx' AND object_id = OBJECT_ID('event_journal'))
    BEGIN
        CREATE INDEX event_journal_slice_idx ON event_journal (slice, entity_type, db_timestamp, seq_nr);
    END;

IF object_id('snapshot') is null
	CREATE TABLE snapshot(
	  slice INT NOT NULL,
	  entity_type NVARCHAR(255) NOT NULL,
	  persistence_id NVARCHAR(255) NOT NULL,
	  seq_nr BIGINT NOT NULL,
	  db_timestamp  datetime2(6),
	  write_timestamp BIGINT NOT NULL,
	  ser_id INTEGER NOT NULL,
	  ser_manifest NVARCHAR(255) NOT NULL,
	  snapshot VARBINARY(MAX) NOT NULL,
	  tags NVARCHAR(255),
	  meta_ser_id INTEGER,
	  meta_ser_manifest NVARCHAR(255),
	  meta_payload VARBINARY(MAX),
	  PRIMARY KEY(persistence_id)
	);

-- `snapshot_slice_idx` is only needed if the slice based queries are used together with snapshot as starting point
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'snapshot_slice_idx' AND object_id = OBJECT_ID('snapshot'))
    BEGIN
        CREATE INDEX snapshot_slice_idx ON snapshot(slice, entity_type, db_timestamp);
    END;

IF object_id('durable_state') is null
	CREATE TABLE durable_state (
	  slice INT NOT NULL,
	  entity_type NVARCHAR(255) NOT NULL,
	  persistence_id NVARCHAR(255) NOT NULL,
	  revision BIGINT NOT NULL,
	  db_timestamp datetime2(6) NOT NULL,

	  state_ser_id INTEGER NOT NULL,
	  state_ser_manifest NVARCHAR(255),
	  state_payload VARBINARY(MAX) NOT NULL,
	  tags NVARCHAR(255),

	  PRIMARY KEY(persistence_id, revision)
	);

-- `durable_state_slice_idx` is only needed if the slice based queries are used
IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'durable_state_slice_idx' AND object_id = OBJECT_ID('durable_state'))
    BEGIN
        CREATE INDEX durable_state_slice_idx ON durable_state(slice, entity_type, db_timestamp, revision);
    END;

-- during creation of all tables below, the following warning is logged:
-- ###################################################################################
-- #  Warning! The maximum key length for a clustered index is 900 bytes.            #
-- #  The index '[some name]' has maximum length of [some  number] bytes.            #
-- #  For some combination of large values, the insert/update operation will fail.   #
-- ###################################################################################

-- Primitive offset types are stored in this table.
-- If only timestamp based offsets are used this table is optional.
-- Configure akka.projection.r2dbc.offset-store.offset-table="" if the table is not created.
IF object_id('akka_projection_offset_store') is null
	CREATE TABLE akka_projection_offset_store (
        projection_name NVARCHAR(255) NOT NULL,
        projection_key NVARCHAR(255) NOT NULL,
        current_offset NVARCHAR(255) NOT NULL,
        manifest NVARCHAR(32) NOT NULL,
        mergeable BIT DEFAULT 0 NOT NULL,
        last_updated BIGINT NOT NULL,
        PRIMARY KEY(projection_name, projection_key)
	);

IF object_id('akka_projection_timestamp_offset_store') is null
    CREATE TABLE akka_projection_timestamp_offset_store (
        projection_name NVARCHAR(255) NOT NULL,
        projection_key NVARCHAR(255) NOT NULL,
        slice INT NOT NULL,
        persistence_id NVARCHAR(255) NOT NULL,
        seq_nr BIGINT NOT NULL,
        -- timestamp_offset is the db_timestamp of the original event
        timestamp_offset datetime2(6) NOT NULL,
        -- timestamp_consumed is when the offset was stored
        -- the consumer lag is timestamp_consumed - timestamp_offset
        timestamp_consumed datetime2(6) NOT NULL,
        PRIMARY KEY(slice, projection_name, timestamp_offset, persistence_id, seq_nr)
    );

IF object_id('akka_projection_management') is null
    CREATE TABLE akka_projection_management (
        projection_name NVARCHAR(255) NOT NULL,
        projection_key NVARCHAR(255) NOT NULL,
        paused BIT NOT NULL,
        last_updated BIGINT NOT NULL,
        PRIMARY KEY(projection_name, projection_key)
    );
