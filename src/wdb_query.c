/*-------------------------------------------------------------------------
 *
 * wdb_query.c
 *
 * Helper functions
 * 
 * Copied and hacked from https://github.com/citusdata/mongo_fdw/blob/master/mongo_query.c
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "wdb_fdw.h"

#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/var.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

/* Local functions forward declarations */
static Expr * FindArgumentOfType(List *argumentList, NodeTag argumentType);
static wg_int WhiteDBOperatorName(const char *operatorName);
static List * EqualityOperatorList(List *operatorList);
static List * UniqueColumnList(List *operatorList);
static List * ColumnOperatorList(Var *column, List *operatorList);

/*
 * FindArgumentOfType walks over the given argument list, looks for an argument
 * with the given type, and returns the argument if it is found.
 */
static Expr *
FindArgumentOfType(List *argumentList, NodeTag argumentType) {
        Expr *foundArgument = NULL;
        ListCell *argumentCell = NULL;

        foreach(argumentCell, argumentList) {
            Expr *argument = (Expr *) lfirst(argumentCell);
            if (nodeTag(argument) == argumentType) {
                foundArgument = argument;
                break;
            }
        }

        return foundArgument;
}


/*
 * WhiteDBOperatorName takes in the given PostgreSQL comparison operator name, and
 * returns its equivalent in WhiteDB.
 */
static wg_int
WhiteDBOperatorName(const char *operatorName) {
    wg_int wgOpName;
    const int32 nameCount = 5;
    static const char *nameMappings[] = { "<", 
                                          ">",
                                          "<=",
                                          ">=",
                                          "<>" };

    static const wg_int nameValues[] = { WG_COND_LESSTHAN,
                                          WG_COND_GREATER,
                                          WG_COND_LTEQUAL,
                                          WG_COND_GTEQUAL,
                                          WG_COND_NOT_EQUAL };

    
    int32 nameIndex = 0;
    for (nameIndex = 0; nameIndex < nameCount; nameIndex++) {
        const char *pgOperatorName = nameMappings[nameIndex];
        if (strncmp(pgOperatorName, operatorName, NAMEDATALEN) == 0) {
            wgOpName = nameValues[nameIndex];
            break;
        }
    }
    
    return wgOpName;
}

/*
 * EqualityOperatorList finds the equality (=) operators in the given list, and
 * returns these operators in a new list.
 */
static List *
EqualityOperatorList(List *operatorList) {
    List *equalityOperatorList = NIL;
    ListCell *operatorCell = NULL;
    
    foreach(operatorCell, operatorList) {
        OpExpr *operator = (OpExpr *) lfirst(operatorCell);
        char *operatorName = NULL;
        
        operatorName = get_opname(operator->opno);
        if (strncmp(operatorName, EQUALITY_OPERATOR_NAME, NAMEDATALEN) == 0) {
            equalityOperatorList = lappend(equalityOperatorList, operator);
        }
    }

    return equalityOperatorList;
}


/*
 * UniqueColumnList walks over the given operator list, and extracts the column
 * argument in each operator. The function then de-duplicates extracted columns,
 * and returns them in a new list.
 */
static List *
UniqueColumnList(List *operatorList) {
    List *uniqueColumnList = NIL;
    ListCell *operatorCell = NULL;
    
    foreach(operatorCell, operatorList) {
        OpExpr *operator = (OpExpr *) lfirst(operatorCell);
        List *argumentList = operator->args;
        Var *column = (Var *) FindArgumentOfType(argumentList, T_Var);
        
        /* list membership is determined via column's equal() function */
        uniqueColumnList = list_append_unique(uniqueColumnList, column);
    }
    
    return uniqueColumnList;
}


/*
 * ColumnOperatorList finds all expressions that correspond to the given column,
 * and returns them in a new list.
 */
static List *
ColumnOperatorList(Var *column, List *operatorList) {
    List *columnOperatorList = NIL;
    ListCell *operatorCell = NULL;
    
    foreach(operatorCell, operatorList)
        {
            OpExpr *operator = (OpExpr *) lfirst(operatorCell);
            List *argumentList = operator->args;
            
            Var *foundColumn = (Var *) FindArgumentOfType(argumentList, T_Var);
            if (equal(column, foundColumn))
                {
                    columnOperatorList = lappend(columnOperatorList, operator);
                }
        }
    
    return columnOperatorList;
}


/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list.
 */
List *
ColumnList(RelOptInfo *baserel) {
    List *columnList = NIL;
    List *neededColumnList = NIL;
    AttrNumber columnIndex = 1;
    AttrNumber columnCount = baserel->max_attr;
    List *targetColumnList = baserel->reltargetlist;
    List *restrictInfoList = baserel->baserestrictinfo;
    ListCell *restrictInfoCell = NULL;
    
    /* first add the columns used in joins and projections */
    neededColumnList = list_copy(targetColumnList);
    
    /* then walk over all restriction clauses, and pull up any used columns */
    foreach(restrictInfoCell, restrictInfoList) {
        RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
        Node *restrictClause = (Node *) restrictInfo->clause;
        List *clauseColumnList = NIL;
        
        /* recursively pull up any columns used in the restriction clause */
        clauseColumnList = pull_var_clause(restrictClause,
                                           PVC_RECURSE_AGGREGATES,
                                           PVC_RECURSE_PLACEHOLDERS);
        
        neededColumnList = list_union(neededColumnList, clauseColumnList);
    }
    
    /* walk over all column definitions, and de-duplicate column list */
    for (columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
        ListCell *neededColumnCell = NULL;
        Var *column = NULL;
        
        /* look for this column in the needed column list */
        foreach(neededColumnCell, neededColumnList) {
            Var *neededColumn = (Var *) lfirst(neededColumnCell);
            if (neededColumn->varattno == columnIndex) {
                column = neededColumn;
                break;
            }
        }
        
        if (column != NULL) {
            columnList = lappend(columnList, column);
        }
    }

    return columnList;
}
