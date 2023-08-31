# Coarse Grained Location Replication

FIXME Outline:

 * drone event producer push destination
 * consuming the events into R2DBC Postgres storage
 * projecting the events into durable state entity
 * additional column querying through grpc service