/*-------------------------------------------------------------------------
 *
 * my Foreign Data Wrapper for PostgreSQL
 *
 * Copyright (c) 2013 Guillaume Lelarge
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Guillaume Lelarge <guillaume@lelarge.info>
 *
 * IDENTIFICATION
 *        simple_fdw/src/simple_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

#include "funcapi.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "utils/rel.h"

#include <sqlite3.h>

PG_MODULE_MAGIC;

/*
 * SQL functions
 */
extern Datum simple_fdw_handler(PG_FUNCTION_ARGS);
extern Datum simple_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(simple_fdw_handler);
PG_FUNCTION_INFO_V1(simple_fdw_validator);

/*
 * Callback functions
 */

/* Planner functions */
#if (PG_VERSION_NUM >= 90200)
static void simpleGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);
static void simpleGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);
static ForeignScan *simpleGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses);
#else
static FdwPlan *simplePlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif

/* Executor reading functions */
static void simpleBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *simpleIterateForeignScan(ForeignScanState *node);
static void simpleReScanForeignScan(ForeignScanState *node);
static void simpleEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static bool myIsValidOption(const char *option, Oid context);
static void simpleGetOptions(Oid foreigntableid, char **database, char **table);

/* 
 * structures used by the FDW 
 *
 * These next two are not actually used by my, but something like this
 * will be needed by anything more complicated that does actual work.
 *
 */

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct myFdwOption
{
	const char	*optname;
	Oid		optcontext;	/* Oid of catalog in which option may appear */
};

/*
 * Describes the valid options for objects that use this wrapper.
 */
static struct myFdwOption valid_options[] =
{

	/* Connection options */
	{ "database",  ForeignServerRelationId },

	/* Table options */
	{ "table",     ForeignTableRelationId },

	/* Sentinel */
	{ NULL,			InvalidOid }
};

/*
 * This is what will be set and stashed away in fdw_private and fetched
 * for subsequent routines.
 */
typedef struct
{
	char	   *foo;
	int			bar;
}	SimpleFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct simpleFdwExecutionState
{
	sqlite3       *conn;
	sqlite3_stmt  *result;
	char          *query;
} SimpleFdwExecutionState;

Datum
simple_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	elog(DEBUG1,"entering function %s",__func__);

	/* assign the handlers for the FDW */
#if (PG_VERSION_NUM >= 90200)
	fdwroutine->GetForeignRelSize = simpleGetForeignRelSize;
	fdwroutine->GetForeignPaths = simpleGetForeignPaths;
	fdwroutine->GetForeignPlan = simpleGetForeignPlan;
#else
	fdwroutine->PlanForeignScan = simplePlanForeignScan;
#endif
	fdwroutine->BeginForeignScan = simpleBeginForeignScan;
	fdwroutine->IterateForeignScan = simpleIterateForeignScan;
	fdwroutine->ReScanForeignScan = simpleReScanForeignScan;
	fdwroutine->EndForeignScan = simpleEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

Datum
simple_fdw_validator(PG_FUNCTION_ARGS)
{
	List      *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid       catalog = PG_GETARG_OID(1);
	ListCell  *cell;
	char      *simple_database = NULL;
	char      *simple_table = NULL;

	elog(DEBUG1,"entering function %s",__func__);

	/*
	 * Check that only options supported by simple_fdw,
	 * and allowed for the current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem	   *def = (DefElem *) lfirst(cell);

		if (!myIsValidOption(def->defname, catalog))
		{
			struct myFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
							 opt->optname);
			}

			ereport(ERROR, 
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME), 
				errmsg("invalid option \"%s\"", def->defname), 
				errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
				));
		}

		if (strcmp(def->defname, "database") == 0)
		{
			if (simple_database)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("redundant options: database (%s)", defGetString(def))
					));

			simple_database = defGetString(def);
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			if (simple_table)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("redundant options: table (%s)", defGetString(def))
					));

			simple_table = defGetString(def);
		}
	}

	PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
myIsValidOption(const char *option, Oid context)
{
	struct myFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a simple_fdw foreign table.
 */
static void
simpleGetOptions(Oid foreigntableid, char **database, char **table)
{
	ForeignTable   *f_table;
	ForeignServer  *f_server;
	List           *options;
	ListCell       *lc;

	/*
	 * Extract options from FDW objects.
	 */
	f_table = GetForeignTable(foreigntableid);
	f_server = GetForeignServer(f_table->serverid);

	options = NIL;
	options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);

	/* Loop through the options */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "database") == 0)
			*database = defGetString(def);

		if (strcmp(def->defname, "table") == 0)
			*table = defGetString(def);
	}

	/* Check we have the options we need to proceed */
	if (!*database && !*table)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("a database and a table must be specified")
			));
}

static void
simpleGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
	SimpleFdwPlanState *fdw_private;

	elog(DEBUG1,"entering function %s",__func__);

	baserel->rows = 0;

	fdw_private = palloc0(sizeof(SimpleFdwPlanState));
	baserel->fdw_private = (void *) fdw_private;
}

static void
simpleGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	Cost		startup_cost,
				total_cost;

	elog(DEBUG1,"entering function %s",__func__);

	startup_cost = 0;
	total_cost = startup_cost + baserel->rows;

	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NIL));		/* no fdw_private data */
}

static ForeignScan *
simpleGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses)
{
	Index		scan_relid = baserel->relid;

	elog(DEBUG1,"entering function %s",__func__);

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,
							NIL);
}

static void
simpleBeginForeignScan(ForeignScanState *node,
						  int eflags)
{
	sqlite3                  *db;
	SimpleFdwExecutionState  *festate;
	char                     *svr_database = NULL;
	char                     *svr_table = NULL;
	char                     *query;
    size_t                   len;

	elog(DEBUG1,"entering function %s",__func__);

	/* Fetch options  */
	simpleGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_database, &svr_table);

	/* Connect to the server */
	if (sqlite3_open(svr_database, &db)) {
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
			errmsg("Can't open sqlite database %s: %s", svr_database, sqlite3_errmsg(db))
			));
		sqlite3_close(db);
	}

	/* Build the query */
    len = strlen(svr_table) + 15;
    query = (char *)palloc(len);
    snprintf(query, len, "SELECT * FROM %s", svr_table);

	/* Stash away the state info we have already */
	festate = (SimpleFdwExecutionState *) palloc(sizeof(SimpleFdwExecutionState));
	node->fdw_state = (void *) festate;
	festate->conn = db;
	festate->result = NULL;
	festate->query = query;
}

static TupleTableSlot *
simpleIterateForeignScan(ForeignScanState *node)
{
	char        **values;
	HeapTuple   tuple;
	int         x;
    const char  *pzTail;
    int         rc;

	SimpleFdwExecutionState *festate = (SimpleFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	elog(DEBUG1,"entering function %s",__func__);

	/* Execute the query, if required */
	if (!festate->result)
	{
		rc = sqlite3_prepare(festate->conn, festate->query, -1, &festate->result, &pzTail);
		if (rc!=SQLITE_OK) {
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("SQL error during prepare: %s", sqlite3_errmsg(festate->conn))
				));
			sqlite3_close(festate->conn);
		}
	}

	ExecClearTuple(slot);

	/* get the next record, if any, and fill in the slot */
	if (sqlite3_step(festate->result) == SQLITE_ROW)
	{
		/* Build the tuple */
		values = (char **) palloc(sizeof(char *) * sqlite3_column_count(festate->result));

		for (x = 0; x < sqlite3_column_count(festate->result); x++)
			values[x] = sqlite3_column_text(festate->result, x);

		tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att), values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}

	/* then return the slot */
	return slot;
}

static void
simpleReScanForeignScan(ForeignScanState *node)
{
	elog(DEBUG1,"entering function %s",__func__);
}

static void
simpleEndForeignScan(ForeignScanState *node)
{
	SimpleFdwExecutionState *festate = (SimpleFdwExecutionState *) node->fdw_state;

	elog(DEBUG1,"entering function %s",__func__);

	if (festate->result)
	{
		sqlite3_finalize(festate->result);
		festate->result = NULL;
	}

	if (festate->conn)
	{
		sqlite3_close(festate->conn);
		festate->conn = NULL;
	}

	if (festate->query)
	{
		pfree(festate->query);
		festate->query = 0;
	}

}
