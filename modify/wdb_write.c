/**
   Code to deal with Mutating the Foreign table. 
   // @TODO, figure all this stuff out later.
 */

static void wdbAddForeignUpdateTargets(Query *parsetree,
        RangeTblEntry *target_rte,
        Relation target_relation);

static List *wdbPlanForeignModify(PlannerInfo *root,
        ModifyTable *plan,
        Index resultRelation,
        int subplan_index);

static void wdbBeginForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *rinfo,
        List *fdw_private,
        int subplan_index,
        int eflags);

static TupleTableSlot *wdbExecForeignInsert(EState *estate,
        ResultRelInfo *rinfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);

static TupleTableSlot *wdbExecForeignUpdate(EState *estate,
        ResultRelInfo *rinfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);

static TupleTableSlot *wdbExecForeignDelete(EState *estate,
        ResultRelInfo *rinfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);

static void wdbEndForeignModify(EState *estate,
        ResultRelInfo *rinfo);

static void wdbExplainForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *rinfo,
        List *fdw_private,
        int subplan_index,
        struct ExplainState *es);


static void
wdbAddForeignUpdateTargets(Query *parsetree,
        RangeTblEntry *target_rte,
        Relation target_relation) {
    /*
     * UPDATE and DELETE operations are performed against rows previously
     * fetched by the table-scanning functions. The FDW may need extra
     * information, such as a row ID or the values of primary-key columns, to
     * ensure that it can identify the exact row to update or delete. To
     * support that, this function can add extra hidden, or "junk", target
     * columns to the list of columns that are to be retrieved from the
     * foreign table during an UPDATE or DELETE.
     *
     * To do that, add TargetEntry items to parsetree->targetList, containing
     * expressions for the extra values to be fetched. Each such entry must be
     * marked resjunk = true, and must have a distinct resname that will
     * identify it at execution time. Avoid using names matching ctidN or
     * wholerowN, as the core system can generate junk columns of these names.
     *
     * This function is called in the rewriter, not the planner, so the
     * information available is a bit different from that available to the
     * planning routines. parsetree is the parse tree for the UPDATE or DELETE
     * command, while target_rte and target_relation describe the target
     * foreign table.
     *
     * If the AddForeignUpdateTargets pointer is set to NULL, no extra target
     * expressions are added. (This will make it impossible to implement
     * DELETE operations, though UPDATE may still be feasible if the FDW
     * relies on an unchanging primary key to identify rows.)
     */

    Form_pg_attribute attr;
    Var *varnode;
    const char *attrname;
    TargetEntry * tle;

    elog(DEBUG1,"entering function %s",__func__);

    attr = RelationGetDescr(target_relation)->attrs[0];

    varnode = makeVar(parsetree->resultRelation,
            attr->attnum,
            attr->atttypid, attr->atttypmod,
            attr->attcollation,
            0);

    /* Wrap it in a resjunk TLE with the right name ... */
    attrname = "key_junk";
    tle = makeTargetEntry((Expr *) varnode,
            list_length(parsetree->targetList) + 1,
            pstrdup(attrname),true);

    /* ... and add it to the query's targetlist */
    parsetree->targetList = lappend(parsetree->targetList, tle);
}


static List * 
wdbPlanForeignModify(PlannerInfo *root,
        ModifyTable *plan,
        Index resultRelation,
        int subplan_index) {

    /*
     * Perform any additional planning actions needed for an insert, update,
     * or delete on a foreign table. This function generates the FDW-private
     * information that will be attached to the ModifyTable plan node that
     * performs the update action. This private information must have the form
     * of a List, and will be delivered to BeginForeignModify during the
     * execution stage.
     *
     * root is the planner's global information about the query. plan is the
     * ModifyTable plan node, which is complete except for the fdwPrivLists
     * field. resultRelation identifies the target foreign table by its
     * rangetable index. subplan_index identifies which target of the
     * ModifyTable plan node this is, counting from zero; use this if you want
     * to index into plan->plans or other substructure of the plan node.
     *
     * If the PlanForeignModify pointer is set to NULL, no additional
     * plan-time actions are taken, and the fdw_private list delivered to
     * BeginForeignModify will be NIL.
     */

    elog(DEBUG1,"entering function %s",__func__);

    return NULL;
}


static void
wdbExplainForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *rinfo,
        List *fdw_private,
        int subplan_index,
        struct ExplainState *es) {
    /*
     * Print additional EXPLAIN output for a foreign table update. This
     * function can call ExplainPropertyText and related functions to add
     * fields to the EXPLAIN output. The flag fields in es can be used to
     * determine what to print, and the state of the ModifyTableState node can
     * be inspected to provide run-time statistics in the EXPLAIN ANALYZE
     * case. The first four arguments are the same as for BeginForeignModify.
     *
     * If the ExplainForeignModify pointer is set to NULL, no additional
     * information is printed during EXPLAIN.
     */

    elog(DEBUG1,"entering function %s",__func__);
}


static void
wdbBeginForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *rinfo,
        List *fdw_private,
        int subplan_index,
        int eflags) {

    /*
     * Begin executing a foreign table modification operation. This routine is
     * called during executor startup. It should perform any initialization
     * needed prior to the actual table modifications. Subsequently,
     * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
     * called for each tuple to be inserted, updated, or deleted.
     *
     * mtstate is the overall state of the ModifyTable plan node being
     * executed; global data about the plan and execution state is available
     * via this structure. rinfo is the ResultRelInfo struct describing the
     * target foreign table. (The ri_FdwState field of ResultRelInfo is
     * available for the FDW to store any private state it needs for this
     * operation.) fdw_private contains the private data generated by
     * PlanForeignModify, if any. subplan_index identifies which target of the
     * ModifyTable plan node this is. eflags contains flag bits describing the
     * executor's operating mode for this plan node.
     *
     * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
     * should not perform any externally-visible actions; it should only do
     * the minimum required to make the node state valid for
     * ExplainForeignModify and EndForeignModify.
     *
     * If the BeginForeignModify pointer is set to NULL, no action is taken
     * during executor startup.
     */
    Relation    rel = rinfo->ri_RelationDesc;
    wdbFdwModifyState *fmstate;
    Form_pg_attribute attr;
    Oid typefnoid;
    bool isvarlena;
    CmdType operation = mtstate->operation;
    int i = 0;
    FmgrInfo* fi;

    elog(DEBUG1,"entering function %s",__func__);

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    fmstate = (wdbFdwModifyState *) palloc0(sizeof(wdbFdwModifyState));
    fmstate->rel = rel;
    fmstate->infoList = NULL;

    if (operation == CMD_UPDATE || operation == CMD_DELETE) {
        /* Find the ctid resjunk column in the subplan's result */
        Plan       *subplan = mtstate->mt_plans[subplan_index]->plan;
        fmstate->key_junk_no = ExecFindJunkAttributeInTlist(subplan->targetlist,
                                                            "key_junk");
        if (!AttributeNumberIsValid(fmstate->key_junk_no))
            elog(ERROR, "could not find key junk column");
    }

    while (1) { // Build up a list of info about all the columns we are working on.
        if (RelationGetDescr(rel)->attrs[i] == NULL) {
            break;
        }

        attr = RelationGetDescr(rel)->attrs[i];
        Assert(!attr->attisdropped);
        getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
        fi = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
        fmgr_info(typefnoid, fi);
        i++;
        fmstate->infoList = lcons(fi, fmstate->infoList);
    }

    initTableOptions(&(fmstate->opt));
    getTableOptions(RelationGetRelid(rel), &(fmstate->opt));

    fmstate->db = GetDatabase(&(fmstate->opt));
    fmstate->record = NULL;

    rinfo->ri_FdwState=fmstate;
}


static TupleTableSlot *
wdbExecForeignInsert(EState *estate,
                     ResultRelInfo *rinfo,
                     TupleTableSlot *slot,
                     TupleTableSlot *planSlot) {
    /*
     * Insert one tuple into the foreign table. estate is global execution
     * state for the query. rinfo is the ResultRelInfo struct describing the
     * target foreign table. slot contains the tuple to be inserted; it will
     * match the rowtype definition of the foreign table. planSlot contains
     * the tuple that was generated by the ModifyTable plan node's subplan; it
     * differs from slot in possibly containing additional "junk" columns.
     * (The planSlot is typically of little interest for INSERT cases, but is
     * provided for completeness.)
     *
     * The return value is either a slot containing the data that was actually
     * inserted (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually inserted
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the INSERT query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignInsert pointer is set to NULL, attempts to insert
     * into the foreign table will fail with an error message.
     *
     */

    char * key_value;
    char * value_value;
    Datum value;
    bool isnull;
    wdbFdwModifyState *fmstate = (wdbFdwModifyState *) rinfo->ri_FdwState;

    elog(DEBUG1,"entering function %s",__func__);

    value = slot_getattr(planSlot, 1, &isnull);
    if(isnull)
        elog(ERROR, "can't get key value");
    key_value = OutputFunctionCall(fmstate->key_info, value);

    value = slot_getattr(planSlot, 2, &isnull);
    if(isnull)
        elog(ERROR, "can't get value value");
    value_value = OutputFunctionCall(fmstate->value_info, value);

    fmstate->record = wg_create_record(fmstate->db, MAX_RECORD_FIELDS);
    if (fmstate->record != NULL) {
        if (wg_set_str_field(fmstate->db, fmstate->record, 0, key_value) != 0) {
            elog(ERROR, "Error from wdb: %s", "Could not update record key");
        }
        if (wg_set_str_field(fmstate->db, fmstate->record, 1, value_value) != 0) {
            elog(ERROR, "Error from wdb: %s", "Could not update record value");
        }
    } else {
        elog(ERROR, "Could not insert into database");
    }

    return slot;
}


static TupleTableSlot *
wdbExecForeignUpdate(EState *estate,
        ResultRelInfo *rinfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot) {

    /*
     * Update one tuple in the foreign table. estate is global execution state
     * for the query. rinfo is the ResultRelInfo struct describing the target
     * foreign table. slot contains the new data for the tuple; it will match
     * the rowtype definition of the foreign table. planSlot contains the
     * tuple that was generated by the ModifyTable plan node's subplan; it
     * differs from slot in possibly containing additional "junk" columns. In
     * particular, any junk columns that were requested by
     * AddForeignUpdateTargets will be available from this slot.
     *
     * The return value is either a slot containing the row as it was actually
     * updated (this might differ from the data supplied, for example as a
     * result of trigger actions), or NULL if no row was actually updated
     * (again, typically as a result of triggers). The passed-in slot can be
     * re-used for this purpose.
     *
     * The data in the returned slot is used only if the UPDATE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignUpdate pointer is set to NULL, attempts to update the
     * foreign table will fail with an error message.
     *
     */
    char * key_value;
    char * key_value_new;
    char * value_value;
    Datum value;
    bool isnull;
    char** values;
    wdbFdwModifyState *fmstate = (wdbFdwModifyState *) rinfo->ri_FdwState;
    wg_int      i = 0;
    HeapTuple	tuple;
    wg_int numFields;

    elog(DEBUG1,"entering function %s",__func__);

    value = ExecGetJunkAttribute(planSlot, fmstate->key_junk_no, &isnull);
    if(isnull)
        elog(ERROR, "can't get junk key value");
    key_value = OutputFunctionCall(fmstate->key_info, value);

    value = slot_getattr(planSlot, 1, &isnull);
    if(isnull)
        elog(ERROR, "can't get new key value");
    key_value_new = OutputFunctionCall(fmstate->key_info, value);

    if(strcmp(key_value, key_value_new) != 0) {
        elog(ERROR, "You cannot update key values (original key value was %s)", key_value);
        return slot;
    }

    value = slot_getattr(planSlot, 2, &isnull);
    if(isnull) {
        elog(ERROR, "can't get value value");
    }

    value_value = OutputFunctionCall(fmstate->value_info, value);
    fmstate->record = wg_find_record_str(fmstate->db, 0, WG_COND_EQUAL, key_value, fmstate->record);
    if (fmstate->record != NULL) {
        if (wg_set_str_field(fmstate->db, fmstate->record, 1, value_value) != 0) {
            elog(ERROR, "Error from wdb: %s", "Could not update record");
        } else {
            numFields = wg_get_record_len(fmstate->db, fmstate->record);
            values = (char **) palloc(sizeof(char *) *numFields);
            
            for (i=0; i<numFields; i++) {
                values[i] = wg_decode_str(fmstate->db, wg_get_field(fmstate->db, fmstate->record, i));
            }
            
            tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(fmstate->rel->rd_att), values);
            ExecStoreTuple(tuple, slot, InvalidBuffer, false);
        }
    } else {
        elog(ERROR, "Could not insert into database");
    }
    
    return slot;
}


static TupleTableSlot *
wdbExecForeignDelete(EState *estate,
        ResultRelInfo *rinfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot) {

    /*
     * Delete one tuple from the foreign table. estate is global execution
     * state for the query. rinfo is the ResultRelInfo struct describing the
     * target foreign table. slot contains nothing useful upon call, but can
     * be used to hold the returned tuple. planSlot contains the tuple that
     * was generated by the ModifyTable plan node's subplan; in particular, it
     * will carry any junk columns that were requested by
     * AddForeignUpdateTargets. The junk column(s) must be used to identify
     * the tuple to be deleted.
     *
     * The return value is either a slot containing the row that was deleted,
     * or NULL if no row was deleted (typically as a result of triggers). The
     * passed-in slot can be used to hold the tuple to be returned.
     *
     * The data in the returned slot is used only if the DELETE query has a
     * RETURNING clause. Hence, the FDW could choose to optimize away
     * returning some or all columns depending on the contents of the
     * RETURNING clause. However, some slot must be returned to indicate
     * success, or the query's reported rowcount will be wrong.
     *
     * If the ExecForeignDelete pointer is set to NULL, attempts to delete
     * from the foreign table will fail with an error message.
     */

    char * key_value;
    Datum value;
    bool isnull;
    char** values;
    wdbFdwModifyState *fmstate = (wdbFdwModifyState *) rinfo->ri_FdwState;
    wg_int      i = 0;
    HeapTuple	tuple;
    wg_int numFields;

    elog(DEBUG1,"entering function %s",__func__);

    value = ExecGetJunkAttribute(planSlot, fmstate->key_junk_no, &isnull);
    if(isnull)
        elog(ERROR, "can't get key value");

    key_value = OutputFunctionCall(fmstate->key_info, value);

    fmstate->record = wg_find_record_str(fmstate->db, 0, WG_COND_EQUAL, key_value, NULL);
    if (fmstate->record != NULL) {

        numFields = wg_get_record_len(fmstate->db, fmstate->record);
        values = (char **) palloc(sizeof(char *) *numFields);
            
        for (i=0; i<numFields; i++) {
            values[i] = wg_decode_str(fmstate->db, wg_get_field(fmstate->db, fmstate->record, i));
        }
            
        tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(fmstate->rel->rd_att), values);
        ExecStoreTuple(tuple, slot, InvalidBuffer, false);

        elog(NOTICE, "DELETED %s", key_value);
        
        if (wg_delete_record(fmstate->db, fmstate->record) != 0) {
            elog(ERROR, "Error from wdb: %s", "Could not delete record");
        }
    } else {
        elog(NOTICE, "NO VAL %s", key_value);
    }

    return slot;
}


static void
wdbEndForeignModify(EState *estate,
        ResultRelInfo *rinfo) {
    /*
     * End the table update and release resources. It is normally not
     * important to release palloc'd memory, but for example open files and
     * connections to remote servers should be cleaned up.
     *
     * If the EndForeignModify pointer is set to NULL, no action is taken
     * during executor shutdown.
     */

    wdbFdwModifyState *fmstate = (wdbFdwModifyState *) rinfo->ri_FdwState;

    elog(DEBUG1,"entering function %s",__func__);

    if(fmstate) {
        if(fmstate->db) {
            ReleaseDatabase(fmstate->db);
            fmstate->db = NULL;
        }
    }
}
