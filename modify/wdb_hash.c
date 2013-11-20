
/*
 * ColumnMappingHash creates a hash table that maps column names to column index
 * and types. This table helps us quickly translate WhiteDB columns to
 * the corresponding PostgreSQL columns.
 */

static HTAB *
ColumnMappingHash(Oid foreignTableId, List *columnList) {

    ListCell *columnCell = NULL;
    const long hashTableSize = 2048;
    HTAB *columnMappingHash = NULL;
    
    HASHCTL hashInfo;
    memset(&hashInfo, 0, sizeof(hashInfo));
    hashInfo.keysize = NAMEDATALEN;
    hashInfo.entrysize = sizeof(ColumnMapping);
    hashInfo.hash = string_hash;
    hashInfo.hcxt = CurrentMemoryContext;
    
    columnMappingHash = hash_create("Column Mapping Hash", hashTableSize, &hashInfo,
                                    (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT));
    Assert(columnMappingHash != NULL);
    
    foreach(columnCell, columnList) {
        Var *column = (Var *) lfirst(columnCell);
        AttrNumber columnId = column->varattno;
        
        ColumnMapping *columnMapping = NULL;
        char *columnName = NULL;
        bool handleFound = false;
        void *hashKey = NULL;
        
        columnName = get_relid_attribute_name(foreignTableId, columnId);
        hashKey = (void *) columnName;
        
        columnMapping = (ColumnMapping *) hash_search(columnMappingHash, hashKey,
                                                      HASH_ENTER, &handleFound);
        Assert(columnMapping != NULL);
        
        columnMapping->columnIndex = columnId - 1;
        columnMapping->columnTypeId = column->vartype;
        columnMapping->columnTypeMod = column->vartypmod;
        columnMapping->columnArrayTypeId = get_element_type(column->vartype);

        elog(NOTICE, "Column %s %d", columnName, columnId);
    }
    
    return columnMappingHash;
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

