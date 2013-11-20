/*-------------------------------------------------------------------------
 *
 * wdb_fdw.h
 *
 *
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef WHITEDB_FDW_H
#define WHITEDB_FDW_H

#include <whitedb/dbapi.h>

#include "fmgr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "utils/datetime.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "utils/timestamp.h"
#include "funcapi.h"


#define EQUALITY_OPERATOR_NAME "="
#define WDB_DEFAULT_ADDRESS "1000"
#define WDB_DEFAULT_SIZE 10485760 // 10MB

/*
 * structures used by the FDW
 *
 * These next two are not actualkly used by wdb, but something like this
 * will be needed by anything more complicated that does actual work.
 *
 */

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct wdbFdwOption {
    const char*         optname;
    Oid			optcontext;  /* Oid of catalog in which option may appear */
};

typedef struct wdbTableOptions
{
    char *address;
    int32_t size;
    Oid serverId;
    Oid userId;
} wdbTableOptions;
/*
 * This is what will be set and stashed away in fdw_private and fetched
 * for subsequent routines.
 */
typedef struct
{
    wdbTableOptions opt;
} wdbFdwPlanState;

typedef struct {
    wdbFdwPlanState plan;
    void* record;
    void* db;
    List* columnMappingList;
    wg_query* query;
    wg_query_arg* queryArguments;
    int numArgumentsInQuery;
    AttInMetadata *attinmeta;
    wg_int lock_id;
} wdbFdwExecState;

typedef struct {
    wdbTableOptions opt;
    void* db;
    void* record;
    Relation rel;
    List *infoList;
    AttrNumber key_junk_no;
} wdbFdwModifyState;

typedef struct {
    Oid serverId;
    Oid userId;
} wdbConnCacheKey;

typedef struct {
  wdbConnCacheKey key;
  void* db;
  int xact_depth;
} wdbConnCacheEntry;

/*
 * ColumnMapping reprents a hash table entry that maps a column name to column
 * related information. We construct these hash table entries to speed up the
 * conversion from WhiteDB records to PostgreSQL tuples; and each hash entry maps
 * the column name to the column's tuple index and its type-related information.
 */
typedef struct ColumnMapping
{
        char* columnName;
        uint32 columnIndex;
        Oid columnTypeId;
        int32 columnTypeMod;
        Oid columnArrayTypeId;

} ColumnMapping;

// Defined elsewhere
extern List* ColumnList(RelOptInfo *baserel);
extern List* ApplicableOpExpressionList(RelOptInfo *baserel);
extern wg_query *BuildWhiteDBQuery(void* db, Oid relationId, List *opExpressionList, int numArgs, wg_query_arg* args);

#endif   /* WHITEDB_FDW_H */
