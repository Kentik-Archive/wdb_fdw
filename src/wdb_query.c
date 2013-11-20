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
static wg_int EncodeConstantValue(void* db, Const *constant);

/*
 * ApplicableOpExpressionList walks over all filter clauses that relate to this
 * foreign table, and chooses applicable clauses that we know we can translate
 * into WhiteDB queries. Currently, these clauses include comparison expressions
 * that have a column and a constant as arguments. For example, "o_orderdate >=
 * date '1994-01-01' + interval '1' year" is an applicable expression.
 */
List *
ApplicableOpExpressionList(RelOptInfo *baserel) {
    List*             opExpressionList = NIL;
    List*             restrictInfoList = baserel->baserestrictinfo;
    ListCell*         restrictInfoCell = NULL;
    
    foreach(restrictInfoCell, restrictInfoList) {
        RestrictInfo*        restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
        Expr*                expression = restrictInfo->clause;
        NodeTag              expressionType = 0;
        OpExpr*              opExpression = NULL;
        char*                operatorName = NULL;
        wg_int               wdbOperatorName = 0;
        List*                argumentList = NIL;
        Var*                 column = NULL;
        Const*               constant = NULL;
        bool                 constantIsArray = false;
        
        /* we only support operator expressions */
        expressionType = nodeTag(expression);
        if (expressionType != T_OpExpr) {
            continue;
        }

        opExpression = (OpExpr *) expression;
        operatorName = get_opname(opExpression->opno);
        
        /* we only support =, <, >, <=, >=, and <> operators */
        wdbOperatorName = WhiteDBOperatorName(operatorName);
        if (wdbOperatorName == 0) {
            continue;
        }

        /*
         * We only support simple binary operators that compare a column against
         * a constant. If the expression is a tree, we don't recurse into it.
         */
        argumentList = opExpression->args;
        column = (Var *) FindArgumentOfType(argumentList, T_Var);
        constant = (Const *) FindArgumentOfType(argumentList, T_Const);
        
        /*
         * We don't push down operators where the constant is an array.
         */
        if (constant != NULL) {
            Oid constantArrayTypeId = get_element_type(constant->consttype);
            if (constantArrayTypeId != InvalidOid) {
                constantIsArray = true;
            }
        }
        
        if (column != NULL && constant != NULL && !constantIsArray) {
            opExpressionList = lappend(opExpressionList, opExpression);
        }
    }

    return opExpressionList;
}

/*
 * BuildWhiteDBQuery takes in the applicable operator expressions for a relation and
 * converts these expressions into equivalent queries in WhiteDB. For now, this
 * function can only transform simple comparison expressions. For example, simple expressions
 * "l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01'" become
 * "l_shipdate: { $gte: new Date(757382400000), $lt: new Date(788918400000) }".
 */
wg_query *
BuildWhiteDBQuery(void* db, Oid relationId, List* opExpressionList, int numExpressions, wg_query_arg* argList) {

    ListCell*          columnCell = NULL;
    int                i = 0;
    wg_query*          query;

    if (numExpressions == 0) {
        return NULL;
    }
    
    /* append equality expressions to the query */
    foreach(columnCell, opExpressionList) {

        OpExpr*               op = (OpExpr *) lfirst(columnCell);
        Oid                   columnId = InvalidOid;
        wg_int                wdbOperatorName;
        char*                 operatorName = NULL;
        List*                 argumentList = op->args;
        Var*                  column = (Var *) FindArgumentOfType(argumentList, T_Var);
        Const*                constant = (Const *) FindArgumentOfType(argumentList, T_Const);
        
        columnId = column->varattno;
        operatorName = get_opname(op->opno);
        wdbOperatorName = WhiteDBOperatorName(operatorName);

        argList[i].column = columnId - 1;
        argList[i].cond = wdbOperatorName;
        argList[i].value = EncodeConstantValue(db, constant);
        i++;
    }

    query = wg_make_query(db, NULL, 0, argList, numExpressions);
    return query;
}

/*
 * EncodeConstantValue encodes the constant as a wg_int
 */
static wg_int
EncodeConstantValue(void* db, Const *constant) {
        Datum       constantValue = constant->constvalue;
        Oid         constantTypeId = constant->consttype;
        bool        constantNull = constant->constisnull;
        wg_int      data = WG_ILLEGAL;

        if (constantNull) {
            return wg_encode_query_param_null(db, NULL);
        }

        switch(constantTypeId) {
                case INT2OID:
                {
                        int16 value = DatumGetInt16(constantValue);
                        data = wg_encode_query_param_int(db, (wg_int) value);
                        break;
                }
                case INT4OID:
                {
                        int32 value = DatumGetInt32(constantValue);
                        data = wg_encode_query_param_int(db, (wg_int) value);
                        break;
                }
                case INT8OID:
                {
                        int64 value = DatumGetInt64(constantValue);
                        data = wg_encode_query_param_int(db, (wg_int) value);
                        break;
                }
                case FLOAT4OID:
                {
                        float4 value = DatumGetFloat4(constantValue);
                        data = wg_encode_query_param_fixpoint(db, (double) value);
                        break;
                }
                case FLOAT8OID:
                {
                        float8 value = DatumGetFloat8(constantValue);
                        data = wg_encode_query_param_double(db, (double) value);
                        break;
                }
                case BPCHAROID:
                case VARCHAROID:
                case TEXTOID:
                {
                        char *outputString = NULL;
                        Oid outputFunctionId = InvalidOid;
                        bool typeVarLength = false;

                        getTypeOutputInfo(constantTypeId, &outputFunctionId, &typeVarLength);
                        outputString = OidOutputFunctionCall(outputFunctionId, constantValue);

                        data = wg_encode_query_param_str(db, outputString, NULL);
                        break;
                }
                default:
                {
                        /*
                         * We currently error out on other data types. Some types such as
                         * byte arrays are easy to add, but they need testing. Other types
                         * such as money or inet, do not have equivalents in MongoDB.
                         */
                        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                                                        errmsg("cannot convert constant value to WhiteDB value"),
                                                        errhint("Constant value data type: %u", constantTypeId)));
                        break;
                }
        }

        return data;
}

/*
 * FindArgumentOfType walks over the given argument list, looks for an argument
 * with the given type, and returns the argument if it is found.
 */
static Expr *
FindArgumentOfType(List *argumentList, NodeTag argumentType) {
    
    Expr*                    foundArgument = NULL;
    ListCell*                argumentCell = NULL;

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
    wg_int wgOpName = 0;
    const int32 nameCount = 6;
    static const char *nameMappings[] = { "=", 
                                          "<",
                                          ">",
                                          "<=",
                                          ">=",
                                          "<>" };

    static const wg_int nameValues[] = { WG_COND_EQUAL,
                                         WG_COND_LESSTHAN,
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
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list.
 */
List *
ColumnList(RelOptInfo *baserel) {
    List*              columnList = NIL;
    List*              neededColumnList = NIL;
    AttrNumber         columnIndex = 1;
    AttrNumber         columnCount = baserel->max_attr;
    List*              targetColumnList = baserel->reltargetlist;
    List*              restrictInfoList = baserel->baserestrictinfo;
    ListCell*          restrictInfoCell = NULL;
    
    /* first add the columns used in joins and projections */
    neededColumnList = list_copy(targetColumnList);
    
    /* then walk over all restriction clauses, and pull up any used columns */
    foreach(restrictInfoCell, restrictInfoList) {
        RestrictInfo*          restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
        Node*                  restrictClause = (Node *) restrictInfo->clause;
        List*                  clauseColumnList = NIL;
        
        /* recursively pull up any columns used in the restriction clause */
        clauseColumnList = pull_var_clause(restrictClause,
                                           PVC_RECURSE_AGGREGATES,
                                           PVC_RECURSE_PLACEHOLDERS);
        
        neededColumnList = list_union(neededColumnList, clauseColumnList);
    }
    
    /* walk over all column definitions, and de-duplicate column list */
    for (columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
        ListCell*          neededColumnCell = NULL;
        Var*               column = NULL;
        
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
