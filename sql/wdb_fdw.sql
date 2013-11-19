/*-------------------------------------------------------------------------
 *
 * WhiteDB Foreign Data Wrapper for PostgreSQL
 *
 *
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION wdb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION wdb_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER wdb_fdw
  HANDLER wdb_fdw_handler
  VALIDATOR wdb_fdw_validator;
