#include "postgres.h"

#include "tcop/utility.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"

#include "access/ivfflat.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "storage/bufmgr.h"
#include "utils/vector.h"
#include "utils/gtype.h"

/* makeRoleSpec
 * Create a RoleSpec with the given type
 */
static RoleSpec *
makeRoleSpec(RoleSpecType type, int location)
{
    RoleSpec *spec = makeNode(RoleSpec);

    spec->roletype = type;
    spec->location = location;

    return spec;
}

static Node *
makeTypeCast(Node *arg, TypeName *typename, int location)
{
    TypeCast *n = makeNode(TypeCast);
    n->arg = arg;
    n->typeName = typename;
    n->location = location;

    return (Node *) n;
}

                    
static Node *                           
makeStringConst(char *str, int location)
{                               
    A_Const *n = makeNode(A_Const);

    n->val.type = T_String;       
    n->val.val.str = str;   
    n->location = location;

    return (Node *)n;               
}                                       
                                        
static Node *                   
makeStringConstCast(char *str, int location, TypeName *typename)
{
        Node *s = makeStringConst(str, location);
                                        
        return makeTypeCast(s, typename, -1);            
}                                 

static char *make_property_alias_for_index(char *var_name) {
    char *str = palloc0(strlen(var_name) + 4);

    str[0] = '"';

    int i = 0;
    for (; i < strlen(var_name); i++)
        str[i + 1] = var_name[i];

    str[i + 1] = '"';

    return str;
}

PG_FUNCTION_INFO_V1(create_ivfflat_ip_ops_index);
Datum create_ivfflat_ip_ops_index(PG_FUNCTION_ARGS)
{
    char *graph;
    Name graph_name;
    char *graph_name_str;
    Oid graph_oid;
    List *parent;

    RangeVar *rv;

    char *label;
    Name label_name;
    char *label_name_str;

    char *property;
    Name property_name;
    char *property_name_str;


    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("graph name must not be NULL")));

    if (PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("label name must not be NULL")));

    if (PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("property name must not be NULL")));

    if (PG_ARGISNULL(3))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dims must not be NULL")));

    if (PG_ARGISNULL(4))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("lists must not be NULL")));


    graph_name = PG_GETARG_NAME(0);
    label_name = PG_GETARG_NAME(1);
    property_name = PG_GETARG_NAME(2);

    graph_name_str = NameStr(*graph_name);
    label_name_str = NameStr(*label_name);
    property_name_str = NameStr(*property_name);

    // Check if graph does not exist
    if (!graph_exists(graph_name_str))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("graph \"%s\" does not exist.", graph_name_str)));

    graph_oid = get_graph_oid(graph_name_str);

    // Check if label with the input name already exists
    if (!label_exists(label_name_str, graph_oid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("label \"%s\" does not exist", label_name_str)));

    //Create the default label tables
    graph = graph_name->data;
    label = label_name->data;
    property = property_name->data;
    PlannedStmt *wrapper;

    DefElem *dims = makeDefElem("dimensions", makeInteger(PG_GETARG_INT32(3)), -1);
    DefElem *lst = makeDefElem("lists", makeInteger(PG_GETARG_INT32(4)), -1);

    ColumnRef  *c = makeNode(ColumnRef);
    c->fields = list_make1(makeString("properties"));
    c->location = -1;

    IndexElem *idx_elem = makeNode(IndexElem);
    idx_elem->name = NULL;
    idx_elem->expr = makeSimpleA_Expr(AEXPR_OP, "->", c, makeStringConstCast(make_property_alias_for_index(property), -1, makeTypeName("gtype")), -1);
    idx_elem->indexcolname = NULL;
    idx_elem->collation = NIL;
    idx_elem->opclass = list_make1(makeString("gtype_ip_ops"));
    idx_elem->opclassopts = NIL;
    idx_elem->ordering = SORTBY_NULLS_DEFAULT;
    idx_elem->nulls_ordering = SORTBY_DEFAULT;

    IndexStmt *idx = makeNode(IndexStmt);
    idx->unique = false;
    idx->concurrent = false;
    idx->idxname = NULL;
    idx->relation = makeRangeVar(graph_name, label_name, -1);
    idx->accessMethod = "ivfflat";
    idx->indexParams = list_make1(idx_elem);
    idx->indexIncludingParams = NULL;
    idx->options = list_make2(dims, lst);
    idx->tableSpace = NULL;
    idx->whereClause = NULL;
    idx->excludeOpNames = NIL;
    idx->idxcomment = NULL;
    idx->indexOid = InvalidOid;
    idx->oldNode = InvalidOid;
    idx->oldCreateSubid = InvalidSubTransactionId;
    idx->oldFirstRelfilenodeSubid = InvalidSubTransactionId;
    idx->primary = false;
    idx->isconstraint = false;
    idx->deferrable = false;
    idx->initdeferred = false;
    idx->transformed = false;
    idx->if_not_exists = false;
    idx->reset_default_tblspc = false;

    wrapper = makeNode(PlannedStmt);
    wrapper->commandType = CMD_UTILITY;
    wrapper->canSetTag = false;
    wrapper->utilityStmt = (Node *)idx;
    wrapper->stmt_location = -1;
    wrapper->stmt_len = 0;


    ProcessUtility(wrapper, "(generated ALTER TABLE ADD CONSTRAINT command)", false, PROCESS_UTILITY_SUBCOMMAND, NULL, NULL, None_Receiver, NULL);

    PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(create_ivfflat_l2_ops_index);
Datum create_ivfflat_l2_ops_index(PG_FUNCTION_ARGS)
{
    char *graph;
    Name graph_name;
    char *graph_name_str;
    Oid graph_oid;
    List *parent;

    RangeVar *rv;

    char *label;
    Name label_name;
    char *label_name_str;

    char *property;
    Name property_name;
    char *property_name_str;


    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("graph name must not be NULL")));

    if (PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("label name must not be NULL")));

    if (PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("property name must not be NULL")));

    if (PG_ARGISNULL(3))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("dims must not be NULL")));

    if (PG_ARGISNULL(4))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("lists must not be NULL")));


    graph_name = PG_GETARG_NAME(0);
    label_name = PG_GETARG_NAME(1);
    property_name = PG_GETARG_NAME(2);

    graph_name_str = NameStr(*graph_name);
    label_name_str = NameStr(*label_name);
    property_name_str = NameStr(*property_name);

    // Check if graph does not exist
    if (!graph_exists(graph_name_str))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("graph \"%s\" does not exist.", graph_name_str)));

    graph_oid = get_graph_oid(graph_name_str);

    // Check if label with the input name already exists
    if (!label_exists(label_name_str, graph_oid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("label \"%s\" does not exist", label_name_str)));

    //Create the default label tables
    graph = graph_name->data;
    label = label_name->data;
    property = property_name->data;
    PlannedStmt *wrapper;

    DefElem *dims = makeDefElem("dimensions", makeInteger(PG_GETARG_INT32(3)), -1);
    DefElem *lst = makeDefElem("lists", makeInteger(PG_GETARG_INT32(4)), -1);

    ColumnRef  *c = makeNode(ColumnRef);
    c->fields = list_make1(makeString("properties"));
    c->location = -1;

    IndexElem *idx_elem = makeNode(IndexElem);
    idx_elem->name = NULL;
    idx_elem->expr = makeSimpleA_Expr(AEXPR_OP, "->", c, makeStringConstCast(make_property_alias_for_index(property), -1, makeTypeName("gtype")), -1);
    idx_elem->indexcolname = NULL;
    idx_elem->collation = NIL;
    idx_elem->opclass = list_make1(makeString("gtype_l2_ops"));
    idx_elem->opclassopts = NIL;
    idx_elem->ordering = SORTBY_NULLS_DEFAULT;
    idx_elem->nulls_ordering = SORTBY_DEFAULT;

    IndexStmt *idx = makeNode(IndexStmt);
    idx->unique = false;
    idx->concurrent = false;
    idx->idxname = NULL;
    idx->relation = makeRangeVar(graph_name, label_name, -1);
    idx->accessMethod = "ivfflat";
    idx->indexParams = list_make1(idx_elem);
    idx->indexIncludingParams = NULL;
    idx->options = list_make2(dims, lst);
    idx->tableSpace = NULL;
    idx->whereClause = NULL;
    idx->excludeOpNames = NIL;
    idx->idxcomment = NULL;
    idx->indexOid = InvalidOid;
    idx->oldNode = InvalidOid;
    idx->oldCreateSubid = InvalidSubTransactionId;
    idx->oldFirstRelfilenodeSubid = InvalidSubTransactionId;
    idx->primary = false;
    idx->isconstraint = false;
    idx->deferrable = false;
    idx->initdeferred = false;
    idx->transformed = false;
    idx->if_not_exists = false;
    idx->reset_default_tblspc = false;

    wrapper = makeNode(PlannedStmt);
    wrapper->commandType = CMD_UTILITY;
    wrapper->canSetTag = false;
    wrapper->utilityStmt = (Node *)idx;
    wrapper->stmt_location = -1;
    wrapper->stmt_len = 0;


    ProcessUtility(wrapper, "(generated ALTER TABLE ADD CONSTRAINT command)", false, PROCESS_UTILITY_SUBCOMMAND, NULL, NULL, None_Receiver, NULL);

    PG_RETURN_VOID();
}



/*
 * Allocate a vector array
 */
VectorArray
VectorArrayInit(int maxlen, int dimensions) {
    VectorArray res = palloc0(sizeof(VectorArrayData));

    res->length = 0;
    res->maxlen = maxlen;
    res->dim = dimensions;
    int gtype_size = VECTOR_SIZE(dimensions) * maxlen;

    res->items = palloc_extended(gtype_size * 2, MCXT_ALLOC_ZERO | MCXT_ALLOC_HUGE);

    for (int i = 0; i < dimensions; i++) {
        gtype *vec = VectorArrayGet(res, i);

        SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
        vec->root.header = dimensions | GT_FEXTENDED_COMPOSITE;
        vec->root.children[0] = GT_HEADER_VECTOR;
    }

    return res;
}

/*
 * Free a vector array
 */
void
VectorArrayFree(VectorArray arr)
{
    pfree(arr->items);
    pfree(arr);
}

/*
 * Get the number of lists in the index
 */
int
IvfflatGetLists(Relation index) {
    IvfflatOptions *opts = (IvfflatOptions *) index->rd_options;

    if (opts)
        return opts->lists;

    return IVFFLAT_DEFAULT_LISTS;
}

int
IvfflatGetDimensions(Relation index)
{
    IvfflatOptions *opts = (IvfflatOptions *) index->rd_options;

    if (opts)
        return opts->dimensions;

    return IVFFLAT_DEFAULT_ELEMENTS;
}


/*
 * Get proc
 */
FmgrInfo *
IvfflatOptionalProcInfo(Relation rel, uint16 procnum) {
     if (!OidIsValid(index_getprocid(rel, 1, procnum)))
          return NULL;

     return index_getprocinfo(rel, 1, procnum);
}

/*
 * Divide by the norm
 *
 * Returns false if value should not be indexed
 *
 * The caller needs to free the pointer stored in value
 * if it's different than the original value
 */
bool
IvfflatNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, gtype *result) {
    double norm = DatumGetFloat8(FunctionCall1Coll(procinfo, collation, *value));

    if (norm > 0) {
         gtype *v = DatumGetVector(*value);

         if (result == NULL)
             result = gtype_value_to_gtype(InitVectorGType(AGT_ROOT_COUNT(v)));

         for (int i = 0; i < AGT_ROOT_COUNT(v); i++)
             result->root.children[1 + (i * sizeof(float8))] = v->root.children[1 + (i * sizeof(float8))] / norm;

         *value = PointerGetDatum(result);

         return true;
    }

    return false;
}

/*
 * New buffer
 */
Buffer
IvfflatNewBuffer(Relation index, ForkNumber forkNum)
{
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    return buf;
}

/*
 * Init page
 */
void
IvfflatInitPage(Buffer buf, Page page)
{
    PageInit(page, BufferGetPageSize(buf), sizeof(IvfflatPageOpaqueData));
    IvfflatPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    IvfflatPageGetOpaque(page)->page_id = IVFFLAT_PAGE_ID;
}

/*
 * Init and register page
 */
void
IvfflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
    *state = GenericXLogStart(index);
    *page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
    IvfflatInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void
IvfflatCommitBuffer(Buffer buf, GenericXLogState *state)
{
    MarkBufferDirty(buf);
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void
IvfflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum) {
    // Get new buffer 
    Buffer newbuf = IvfflatNewBuffer(index, forkNum);
    Page newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

    // Update the previous buffer 
    IvfflatPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

    // Init new page 
    IvfflatInitPage(newbuf, newpage);

    // Commit 
    MarkBufferDirty(*buf);
    MarkBufferDirty(newbuf);
    GenericXLogFinish(*state);

    // Unlock 
    UnlockReleaseBuffer(*buf);

    *state = GenericXLogStart(index);
    *page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
    *buf = newbuf;
}

/*
 * Update the start or insert page of a list
 */
void
IvfflatUpdateList(Relation index, ListInfo listInfo, BlockNumber insertPage, BlockNumber originalInsertPage, BlockNumber startPage, ForkNumber forkNum) {
    Buffer buf;
    Page page;
    GenericXLogState *state;
    IvfflatList list;
    bool changed = false;

    buf = ReadBufferExtended(index, forkNum, listInfo.blkno, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    list = (IvfflatList) PageGetItem(page, PageGetItemId(page, listInfo.offno));

    if (BlockNumberIsValid(insertPage) && insertPage != list->insertPage)
    {
        // Skip update if insert page is lower than original insert page  
        // This is needed to prevent insert from overwriting vacuum 
        if (!BlockNumberIsValid(originalInsertPage) || insertPage >= originalInsertPage)
        {
            list->insertPage = insertPage;
            changed = true;
        }
    }

    if (BlockNumberIsValid(startPage) && startPage != list->startPage)
    {
        list->startPage = startPage;
        changed = true;
    }

    // Only commit if changed 
    if (changed) {
         IvfflatCommitBuffer(buf, state);
    } else {
         GenericXLogAbort(state);
         UnlockReleaseBuffer(buf);
    }
}
