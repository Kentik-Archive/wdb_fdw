/*-------------------------------------------------------------------------
 *
 * WhiteDB Foreign Data Wrapper for PostgreSQL
 *
 * Copyright (c) 2013 CloudHelix
 *
 * This software is released under the MIT Licence
 *
 * Author: Ian Pye <ian@chimera.io>
 *
 * IDENTIFICATION
 *        wdb_fdw/src/wdb_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "wdb_fdw.h"

#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"


#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "nodes/makefuncs.h"
#include "utils/memutils.h"

#include "access/xact.h"

PG_MODULE_MAGIC;

//taken from redis_fdw
#define PROCID_TEXTEQ 67
//#define DEBUG 1
#define MAX_RECORD_FIELDS 2

/*
 * SQL functions
 */
extern Datum wdb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum wdb_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(wdb_fdw_handler);
PG_FUNCTION_INFO_V1(wdb_fdw_validator);

static struct wdbFdwOption valid_options[] =
{
    /* Connection options */
    {"address", ForeignServerRelationId},
    {"size", ForeignServerRelationId},
    /* Sentinel */
    {NULL, InvalidOid}
};

/* callback functions */
static void wdbGetForeignRelSize(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid);

static void wdbGetForeignPaths(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid);

static ForeignScan *wdbGetForeignPlan(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid,
        ForeignPath *best_path,
        List *tlist,
        List *scan_clauses);

static void wdbBeginForeignScan(ForeignScanState *node,
        int eflags);

static TupleTableSlot *wdbIterateForeignScan(ForeignScanState *node);

static void wdbReScanForeignScan(ForeignScanState *node);

static void wdbEndForeignScan(ForeignScanState *node);

static void wdbExplainForeignScan(ForeignScanState *node,
        struct ExplainState *es);

static bool wdbAnalyzeForeignTable(Relation relation,
        AcquireSampleRowsFunc *func,
        BlockNumber *totalpages);

static HTAB *ConnectionHash = NULL;

void initTableOptions(struct wdbTableOptions *table_options);
void* GetDatabase(struct wdbTableOptions *table_options);
void ReleaseDatabase(void *db);

// Quick and dirty calls to populate a tuple.
static List *ColumnMappingList(Oid foreignTableId, List *columnList);
static Datum ColumnValue(void *db, wg_int data, Oid columnTypeId, int32 columnTypeMod);
static void FillTupleSlot(void *db, void *record, List *columnMappingList, Datum *columnValues, bool *columnNulls);
static bool ColumnTypesCompatible(wg_int wgType, Oid columnTypeId);

Datum
wdb_fdw_handler(PG_FUNCTION_ARGS) {

    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    elog(DEBUG1,"entering function %s",__func__);

    /* assign the handlers for the FDW */

    /* these are required */
    fdwroutine->GetForeignRelSize = wdbGetForeignRelSize;
    fdwroutine->GetForeignPaths = wdbGetForeignPaths;
    fdwroutine->GetForeignPlan = wdbGetForeignPlan;
    fdwroutine->BeginForeignScan = wdbBeginForeignScan;
    fdwroutine->IterateForeignScan = wdbIterateForeignScan;
    fdwroutine->ReScanForeignScan = wdbReScanForeignScan;
    fdwroutine->EndForeignScan = wdbEndForeignScan;

    /* remainder are optional - use NULL if not required */
    /* support for insert / update / delete */
    /**
    fdwroutine->AddForeignUpdateTargets = wdbAddForeignUpdateTargets;
    fdwroutine->PlanForeignModify = wdbPlanForeignModify;
    fdwroutine->BeginForeignModify = wdbBeginForeignModify;
    fdwroutine->ExecForeignInsert = wdbExecForeignInsert;
    fdwroutine->ExecForeignUpdate = wdbExecForeignUpdate;
    fdwroutine->ExecForeignDelete = wdbExecForeignDelete;
    fdwroutine->EndForeignModify = wdbEndForeignModify;
    */

    /* support for EXPLAIN */
    fdwroutine->ExplainForeignScan = wdbExplainForeignScan;
    //fdwroutine->ExplainForeignModify = wdbExplainForeignModify;

    /* support for ANALYSE */
    fdwroutine->AnalyzeForeignTable = wdbAnalyzeForeignTable;

    PG_RETURN_POINTER(fdwroutine);
}

static bool isValidOption(const char *option, Oid context) {

    struct wdbFdwOption *opt;
    
#ifdef DEBUG
    elog(NOTICE, "isValidOption %s", option);
#endif
    
    for (opt = valid_options; opt->optname; opt++){
        if (context == opt->optcontext && strcmp(opt->optname, option) == 0) {
            return true;
        }
    }
    return false;
}

void * GetDatabase(struct wdbTableOptions *table_options) {
    bool                 found;
    wdbConnCacheEntry*   entry;
    wdbConnCacheKey      key;

    /* First time through, initialize connection cache hashtable */
    if (ConnectionHash == NULL)
    {
        HASHCTL     ctl;

        MemSet(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(wdbConnCacheKey);
        ctl.entrysize = sizeof(wdbConnCacheEntry);
        ctl.hash = tag_hash;
        /* allocate ConnectionHash in the cache context */
        ctl.hcxt = CacheMemoryContext;
        ConnectionHash = hash_create("wdb_fdw connections", 8,
                                     &ctl,
                                     HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }
    
    /* Create hash key for the entry.  Assume no pad bytes in key struct */
    key.serverId = table_options->serverId;
    key.userId = table_options->userId;
    
    /*
     * Find or create cached entry for requested connection.
     */
    entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
    if (!found) {
        /* initialize new hashtable entry (key is already filled in) */
        entry->db = NULL;
        entry->xact_depth = 0;
    }

    if (entry->db == NULL) {
        entry->db = wg_attach_database(table_options->address, table_options->size);
        if(!entry->db) {
            elog(ERROR,"Could not attach to database");
        }
    }

    return entry->db;
 }

void ReleaseDatabase(void *db) {
    /* keep arround for next connection */
    // wg_detach_database(db);
}

void initTableOptions(struct wdbTableOptions *table_options) {
    table_options->address = NULL;
    table_options->size = 0;
}

static void
getTableOptions(Oid foreigntableid, struct wdbTableOptions *table_options) {
    ForeignTable*       table;
    ForeignServer*      server;
    UserMapping*        mapping;
    List*               options = NIL;
    ListCell*           lc = NULL;

#ifdef DEBUG
    elog(NOTICE, "getTableOptions");
#endif

    /*
     * Extract options from FDW objects.
     *
     */
    table = GetForeignTable(foreigntableid);
    server = GetForeignServer(table->serverid);
    mapping = GetUserMapping(GetUserId(), table->serverid);

    table_options->userId = mapping->userid;
    table_options->serverId = server->serverid;

    options = NIL;
    options = list_concat(options, table->options);
    options = list_concat(options, server->options);
    options = list_concat(options, mapping->options);

    /* Loop through the options, and get the server/port */
    foreach(lc, options)
        {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "address") == 0)
            table_options->address = defGetString(def);

        if (strcmp(def->defname, "size") == 0)
            table_options->size = atoi(defGetString(def));
    }

    /* Default values, if required */
    if (!table_options->address)
        table_options->address = WDB_DEFAULT_ADDRESS;
    if(!table_options->size)
        table_options->size = WDB_DEFAULT_SIZE;

}

Datum
wdb_fdw_validator(PG_FUNCTION_ARGS) {
    List*          options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid            catalog = PG_GETARG_OID(1);
    ListCell*      cell;

    /* used for detecting duplicates; does not remember vals */
    struct wdbTableOptions table_options;

    elog(DEBUG1,"entering function %s",__func__);

    initTableOptions(&table_options);

    foreach(cell, options_list) {
        DefElem *def = (DefElem*) lfirst(cell);

        /*check that this is in the list of known options*/
        if(!isValidOption(def->defname, catalog)) {
            struct wdbFdwOption *opt;
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
                     errhint("Valid options in this context are: %s",
                         buf.len ? buf.data : "<none>")
                    ));
        }


        /*make sure options don't repeat */
        if(strcmp(def->defname, "address") == 0) {
            if (table_options.address)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("conflicting or redundant options: "
                                "address (%s)", defGetString(def))
                                ));
            
            table_options.address = defGetString(def);
        }

        if(strcmp(def->defname, "size") == 0) {
            if (table_options.size)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("conflicting or redundant options: "
                                       "size (%s)", defGetString(def))
                                ));
            
            table_options.size = atoi(defGetString(def));
        }
    }

    PG_RETURN_VOID();
}

static void wdbGetForeignRelSize(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid) {
    /*
     * Obtain relation size estimates for a foreign table. This is called at
     * the beginning of planning for a query that scans a foreign table. root
     * is the planner's global information about the query; baserel is the
     * planner's information about this table; and foreigntableid is the
     * pg_class OID of the foreign table. (foreigntableid could be obtained
     * from the planner data structures, but it's passed explicitly to save
     * effort.)
     *
     * This function should update baserel->rows to be the expected number of
     * rows returned by the table scan, after accounting for the filtering
     * done by the restriction quals. The initial value of baserel->rows is
     * just a constant default estimate, which should be replaced if at all
     * possible. The function may also choose to update baserel->width if it
     * can compute a better estimate of the average result row width.
     */

    wdbFdwPlanState *fdw_private;
    void* db;

    elog(DEBUG1,"entering function %s",__func__);

    baserel->rows = 0;

    fdw_private = palloc(sizeof(wdbFdwPlanState));
    baserel->fdw_private = (void *) fdw_private;

    initTableOptions(&(fdw_private->opt));
    getTableOptions(foreigntableid, &(fdw_private->opt));

    /* initialize reuired state in fdw_private */

    db = GetDatabase(&(fdw_private->opt));

    // @TODO -- make this actually work.
    // baserel->rows = 0;

    ReleaseDatabase(db);
}

static void
wdbGetForeignPaths(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid) {

    /*
     * Create possible access paths for a scan on a foreign table. This is
     * called during query planning. The parameters are the same as for
     * GetForeignRelSize, which has already been called.
     *
     * This function must generate at least one access path (ForeignPath node)
     * for a scan on the foreign table and must call add_path to add each such
     * path to baserel->pathlist. It's recommended to use
     * create_foreignscan_path to build the ForeignPath nodes. The function
     * can generate multiple access paths, e.g., a path which has valid
     * pathkeys to represent a pre-sorted result. Each access path must
     * contain cost estimates, and can contain any FDW-private information
     * that is needed to identify the specific scan method intended.
     */

    Cost startup_cost, total_cost;

    elog(DEBUG1,"entering function %s",__func__);

    startup_cost = 10;

    total_cost = startup_cost + baserel->rows;

    /* Create a ForeignPath node and add it as only possible path */
    add_path(baserel, (Path *)
             create_foreignscan_path(root, baserel,
                                     baserel->rows,
                                     startup_cost,
                                     total_cost,
                                     NIL,    /* no pathkeys */
                                     NULL,   /* no outer rel either */
                                     NIL));  /* no fdw_private data */
}



static ForeignScan *
wdbGetForeignPlan(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreignTableId,
        ForeignPath *best_path,
        List *tlist,
        List *scan_clauses) {
    
    /*
     * Create a ForeignScan plan node from the selected foreign access path.
     * This is called at the end of query planning. The parameters are as for
     * GetForeignRelSize, plus the selected ForeignPath (previously produced
     * by GetForeignPaths), the target list to be emitted by the plan node,
     * and the restriction clauses to be enforced by the plan node.
     *
     * This function must create and return a ForeignScan plan node; it's
     * recommended to use make_foreignscan to build the ForeignScan node.
     *
     */

    Index                  scan_relid = baserel->relid;
    List*                  columnList = NIL;
    List*                  opExpressionList = NIL;
    List*                  foreignPrivateList = NIL;

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check. So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */

    elog(DEBUG1,"entering function %s",__func__);

    /*
     * We push down applicable restriction clauses to WhiteDB, but for simplicity
     * we currently put all the restrictionClauses into the plan node's qual
     * list for the executor to re-check. So all we have to do here is strip
     * RestrictInfo nodes from the clauses and ignore pseudoconstants (which
     * will be handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    // Pull out some needed info.
    columnList = ColumnList(baserel);
    opExpressionList = ApplicableOpExpressionList(baserel);

    // We will save this for execution later.
    foreignPrivateList = list_make2(columnList, opExpressionList);

    /* Create the ForeignScan node */
    return make_foreignscan(tlist,
            scan_clauses,
            scan_relid,
            NIL, /* no expressions to evaluate */
            foreignPrivateList); 
}


static void
wdbBeginForeignScan(ForeignScanState *node, int eflags) {

    /*
     * Begin executing a foreign scan. This is called during executor startup.
     * It should perform any initialization needed before the scan can start,
     * but not start executing the actual scan (that should be done upon the
     * first call to IterateForeignScan). The ForeignScanState node has
     * already been created, but its fdw_state field is still NULL.
     * Information about the table to scan is accessible through the
     * ForeignScanState node (in particular, from the underlying ForeignScan
     * plan node, which contains any FDW-private information provided by
     * GetForeignPlan). eflags contains flag bits describing the executor's
     * operating mode for this plan node.
     *
     * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
     * should not perform any externally-visible actions; it should only do
     * the minimum required to make the node state valid for
     * ExplainForeignScan and EndForeignScan.
     *
     */

    wdbFdwExecState*             estate;
    List*                        columnMappingList = NIL;
    List*                        foreignPrivateList = NIL;
    List*                        opExpressionList = NIL;
    Oid                          foreignTableId = InvalidOid;
    ForeignScan*                 foreignScan = NULL;

    elog(DEBUG1,"entering function %s",__func__);

    estate = (wdbFdwExecState*) palloc(sizeof(wdbFdwExecState));
    estate->record = NULL;
    estate->db = NULL;
    estate->query = NULL;
    estate->queryArguments = NULL;
    estate->numArgumentsInQuery = 0;
    estate->lock_id = 0;
    node->fdw_state = (void *) estate;

    foreignTableId = RelationGetRelid(node->ss.ss_currentRelation);
    foreignScan = (ForeignScan *) node->ss.ps.plan;

    foreignPrivateList = foreignScan->fdw_private;
    Assert(list_length(foreignPrivateList) == 2);

    initTableOptions(&(estate->plan.opt));
    getTableOptions(RelationGetRelid(node->ss.ss_currentRelation), &(estate->plan.opt));

    /* initialize required state in fdw_private */
    estate->db = GetDatabase(&(estate->plan.opt));

    if (estate->db == NULL) {
        elog(ERROR,"Could not connect to a WhiteDB Database");
    }

    // Aquire a read lock for the duration of the scan.
    estate->lock_id = wg_start_read(estate->db);

    /* OK, we connected. If this is an EXPLAIN, bail out now */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    estate->attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);

    columnMappingList = ColumnMappingList(foreignTableId, (List *)linitial(foreignPrivateList));
    estate->columnMappingList = columnMappingList;

    // Anything to query here?
    opExpressionList = (List *)lsecond(foreignPrivateList);
    estate->numArgumentsInQuery = list_length(opExpressionList);
    if (estate->numArgumentsInQuery > 0) {
        estate->queryArguments = (wg_query_arg*) palloc0(sizeof(wg_query_arg) * estate->numArgumentsInQuery);
        estate->query = BuildWhiteDBQuery(estate->db, foreignTableId, opExpressionList, estate->numArgumentsInQuery, estate->queryArguments);
    }
}

// @TODO -- push down where clause to construct a query to run on.
static TupleTableSlot *
wdbIterateForeignScan(ForeignScanState *node) {
    /*
     * Fetch one row from the foreign source, returning it in a tuple table
     * slot (the node's ScanTupleSlot should be used for this purpose). Return
     * NULL if no more rows are available. The tuple table slot infrastructure
     * allows either a physical or virtual tuple to be returned; in most cases
     * the latter choice is preferable from a performance standpoint. Note
     * that this is called in a short-lived memory context that will be reset
     * between invocations. Create a memory context in BeginForeignScan if you
     * need longer-lived storage, or use the es_query_cxt of the node's
     * EState.
     *
     * The rows returned must match the column signature of the foreign table
     * being scanned. If you choose to optimize away fetching columns that are
     * not needed, you should insert nulls in those column positions.
     *
     * Note that PostgreSQL's executor doesn't care whether the rows returned
     * violate any NOT NULL constraints that were defined on the foreign table
     * columns — but the planner does care, and may optimize queries
     * incorrectly if NULL values are present in a column declared not to
     * contain them. If a NULL value is encountered when the user has declared
     * that none should be present, it may be appropriate to raise an error
     * (just as you would need to do in the case of a data type mismatch).
     */

    wdbFdwExecState*        estate = (wdbFdwExecState *) node->fdw_state;
    List*                   columnMappingList = estate->columnMappingList;
    TupleTableSlot*         slot = node->ss.ss_ScanTupleSlot;
    TupleDesc               tupleDescriptor = slot->tts_tupleDescriptor;
    Datum*                  columnValues = slot->tts_values;
    bool*                   columnNulls = slot->tts_isnull;
    int32                   columnCount = tupleDescriptor->natts;

    elog(DEBUG1,"entering function %s",__func__);

    // First clear this guy out.
    ExecClearTuple(slot);

    /* initialize all values for this row to null */
    memset(columnValues, 0, columnCount * sizeof(Datum));
    memset(columnNulls, true, columnCount * sizeof(bool));
    
    // Pull the next record, whether we use a query or not.
    if (estate->query != NULL) {
        estate->record = wg_fetch(estate->db, estate->query);
    } else if (estate->record == NULL) {
        estate->record = wg_get_first_record(estate->db);
    } else {
        estate->record = wg_get_next_record(estate->db, estate->record);
    }

    if (estate->record != NULL) {
        // Copy the record from WhiteDB state to a PG tuple.
        FillTupleSlot(estate->db, estate->record, columnMappingList, columnValues, columnNulls);
        ExecStoreVirtualTuple(slot);
    }

    /* then return the slot */
    return slot;
}


static void
wdbReScanForeignScan(ForeignScanState *node) {
    /*
     * Restart the scan from the beginning. Note that any parameters the scan
     * depends on may have changed value, so the new scan does not necessarily
     * return exactly the same rows.
     */

    elog(DEBUG1,"entering function %s",__func__);

}


static void
wdbEndForeignScan(ForeignScanState *node) {
    /*
     * End the scan and release resources. It is normally not important to
     * release palloc'd memory, but for example open files and connections to
     * remote servers should be cleaned up.
     */

    wdbFdwExecState *estate = (wdbFdwExecState *) node->fdw_state;

    elog(DEBUG1,"entering function %s",__func__);

    if(estate) {
        
        // End the lock.
        wg_end_read(estate->db, estate->lock_id);

        // Free any query structures we have.
        if (estate->query != NULL) {
            wg_free_query(estate->db, estate->query);
            for (int i=0; i<estate->numArgumentsInQuery; i++) {
                wg_free_query_param(estate->db, estate->queryArguments[i].value);
            }
        }
        
        if(estate->db){
            ReleaseDatabase(estate->db);
            estate->db = NULL;
        }
    }
}

///// Now getting into the INSERT / UPDATE / DELETE parts.
// @TODO -- taken out for now.

static void
wdbExplainForeignScan(ForeignScanState *node,
        struct ExplainState *es) {
    /*
     * Print additional EXPLAIN output for a foreign table scan. This function
     * can call ExplainPropertyText and related functions to add fields to the
     * EXPLAIN output. The flag fields in es can be used to determine what to
     * print, and the state of the ForeignScanState node can be inspected to
     * provide run-time statistics in the EXPLAIN ANALYZE case.
     *
     * If the ExplainForeignScan pointer is set to NULL, no additional
     * information is printed during EXPLAIN.
     */

    elog(DEBUG1,"entering function %s",__func__);

}

static bool
wdbAnalyzeForeignTable(Relation relation,
        AcquireSampleRowsFunc *func,
        BlockNumber *totalpages) {
    /* ----
     * This function is called when ANALYZE is executed on a foreign table. If
     * the FDW can collect statistics for this foreign table, it should return
     * true, and provide a pointer to a function that will collect sample rows
     * from the table in func, plus the estimated size of the table in pages
     * in totalpages. Otherwise, return false.
     *
     * If the FDW does not support collecting statistics for any tables, the
     * AnalyzeForeignTable pointer can be set to NULL.
     *
     * If provided, the sample collection function must have the signature:
     *
     *	  int
     *	  AcquireSampleRowsFunc (Relation relation, int elevel,
     *							 HeapTuple *rows, int targrows,
     *							 double *totalrows,
     *							 double *totaldeadrows);
     *
     * A random sample of up to targrows rows should be collected from the
     * table and stored into the caller-provided rows array. The actual number
     * of rows collected must be returned. In addition, store estimates of the
     * total numbers of live and dead rows in the table into the output
     * parameters totalrows and totaldeadrows. (Set totaldeadrows to zero if
     * the FDW does not have any concept of dead rows.)
     * ----
     */
    elog(DEBUG1,"entering function %s",__func__);
    return false;
}


//////// Static Helper Functions need to be here.

static List *
ColumnMappingList(Oid foreignTableId, List *columnList) {

    ListCell *columnCell = NULL;
    List *columnMappingList = NIL;
        
    foreach(columnCell, columnList) {
        Var *column = (Var *) lfirst(columnCell);
        AttrNumber columnId = column->varattno;
        
        ColumnMapping *columnMapping = NULL;
        char *columnName = NULL;
        
        columnName = get_relid_attribute_name(foreignTableId, columnId);
        
        columnMapping = (ColumnMapping *)  palloc0(sizeof(ColumnMapping));
        Assert(columnMapping != NULL);
        
        columnMapping->columnName = pstrdup(columnName);
        columnMapping->columnIndex = columnId - 1;
        columnMapping->columnTypeId = column->vartype;
        columnMapping->columnTypeMod = column->vartypmod;
        columnMapping->columnArrayTypeId = get_element_type(column->vartype);

        // Append to our list.
        columnMappingList = lcons(columnMapping, columnMappingList);
    }
    
    return columnMappingList;
}

/**
   Given a WhiteDB record, translates it to a PG Tuple.
 */
static void
FillTupleSlot(void *db, void *record,
              List *columnMappingList, Datum *columnValues, bool *columnNulls) {

    wg_int numFields = wg_get_record_len(db, record);
    ListCell *columnCell = NULL;
            
    foreach(columnCell, columnMappingList) {
        ColumnMapping*          columnMapping = lfirst(columnCell);
        Oid                     columnTypeId = InvalidOid;
        Oid                     columnTypeMod = InvalidOid;
        bool                    compatibleTypes = false;
        int32                   columnIndex = 0;
        wg_int                  data;
        wg_int                  wgType;

        if (columnMapping == NULL) {
            continue;
        }

        columnIndex = columnMapping->columnIndex;
        columnTypeMod = columnMapping->columnTypeMod;
        columnTypeId = columnMapping->columnTypeId;

        // Don't go off the end of this record.
        if (columnIndex >= numFields) {
            continue;
        }

        data = wg_get_field(db, record, columnIndex);
        wgType = wg_get_field_type(db, record, columnIndex); // returns 0 when error
        compatibleTypes = ColumnTypesCompatible(wgType, columnTypeId);

        /* if types are incompatible, leave this column null */
        if (!compatibleTypes) {
            continue;
        }
        
        // If we are here, we can actually populate the row now.
        columnValues[columnIndex] = ColumnValue(db, data, columnTypeId, columnTypeMod);
        columnNulls[columnIndex] = false;
    }
}

/*
 * ColumnTypesCompatible checks if the given WhiteDB type can be converted to the
 * given PostgreSQL type. 
 */
static bool
ColumnTypesCompatible(wg_int wgType, Oid columnTypeId)
{
        bool compatibleTypes = false;

        /* we consider the PostgreSQL column type as authoritative */
        switch(columnTypeId)
        {
                case INT2OID: case INT4OID:
                case INT8OID: case FLOAT4OID:
                case FLOAT8OID: case NUMERICOID:
                {
                        if (wgType == WG_INTTYPE || wgType == WG_DOUBLETYPE ||
                                wgType == WG_FIXPOINTTYPE)
                        {
                                compatibleTypes = true;
                        }
                        break;
                }
                case BPCHAROID:
                case VARCHAROID:
                case TEXTOID:
                {
                        if (wgType == WG_STRTYPE)
                        {
                                compatibleTypes = true;
                        }
                        break;
                }
                default:
                {
                        /*
                         * We currently error out on other data types. Some types such as
                         * byte arrays are easy to add, but they need testing. 
                         */
                        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                                                        errmsg("cannot convert wgtype type to column type"),
                                                        errhint("Column type: %u", (uint32) columnTypeId)));
                        break;
                }
        }

        return compatibleTypes;
}

/*
 * ColumnValue uses column type information to read the current value pointed to
 * by the record and field, and converts this value to the corresponding PostgreSQL
 * datum. The function then returns this datum.
 */
static Datum
ColumnValue(void *db, wg_int data, Oid columnTypeId, int32 columnTypeMod)
{
        Datum columnValue = 0;

        switch(columnTypeId)
        {
                case INT2OID:
                {
                    int16 value = (int16) wg_decode_int(db, data);
                    columnValue = Int16GetDatum(value);
                    break;
                }
                case INT4OID:
                {
                    int32 value = wg_decode_int(db, data);
                    columnValue = Int32GetDatum(value);
                    break;
                }
                case INT8OID:
                {
                    int64 value = wg_decode_int(db, data);
                    columnValue = Int64GetDatum(value);
                    break;
                }
                case FLOAT4OID:
                {
                    float4 value = (float4) wg_decode_fixpoint(db, data);
                    columnValue = Float4GetDatum(value);
                    break;
                }
                case FLOAT8OID:
                {
                    float8 value = wg_decode_double(db, data);
                    columnValue = Float8GetDatum(value);
                    break;
                }
                case NUMERICOID:
                {
                        float8 value = wg_decode_double(db, data);
                        Datum valueDatum = Float8GetDatum(value);

                        /* overlook type modifiers for numeric */
                        columnValue = DirectFunctionCall1(float8_numeric, valueDatum);
                        break;
                }
                case BPCHAROID:
                {
                    const char *value = wg_decode_str(db, data);
                    Datum valueDatum = CStringGetDatum(value);

                    columnValue = DirectFunctionCall3(bpcharin, valueDatum,
                                                      ObjectIdGetDatum(InvalidOid),
                                                      Int32GetDatum(columnTypeMod));
                    break;
                }
                case VARCHAROID:
                {
                    const char *value = wg_decode_str(db, data);
                    Datum valueDatum = CStringGetDatum(value);
                    
                    columnValue = DirectFunctionCall3(varcharin, valueDatum,
                                                      ObjectIdGetDatum(InvalidOid),
                                                      Int32GetDatum(columnTypeMod));
                    break;
                }
                case TEXTOID:
                {
                        const char *value = wg_decode_str(db, data);
                        columnValue = CStringGetTextDatum(value);
                        break;
                }
                default:
                {
                        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                                                        errmsg("cannot convert bson type to column type"),
                                                        errhint("Column type: %u", (uint32) columnTypeId)));
                        break;
                }
        }

        return columnValue;
}
