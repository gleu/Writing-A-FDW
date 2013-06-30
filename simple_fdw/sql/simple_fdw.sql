/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper  my
 *
 * Copyright (c) 2013, Guillaume Lelarge
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Guillaume Lelarge <guillaume@lelarge.info>
 *
 * IDENTIFICATION
 *                simple_fdw/=sql/simple_fdw.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION simple_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION simple_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER simple_fdw
  HANDLER simple_fdw_handler
  VALIDATOR simple_fdw_validator;
