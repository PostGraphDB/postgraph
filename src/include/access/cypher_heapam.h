/*-------------------------------------------------------------------------
 *
 * heapam.h
 *	  POSTGRES heap access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/heapam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CYPHER_HEAPAM_H
#define CYPHER_HEAPAM_H

#include "access/heapam.h"

#include "access/relation.h"	/* for backward compatibility */
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"		/* for backward compatibility */
#include "access/tableam.h"
#include "nodes/lockoptions.h"
#include "nodes/primnodes.h"
#include "storage/bufpage.h"
#include "storage/dsm.h"
#include "storage/lockdefs.h"
#include "storage/shm_toc.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"


typedef enum CYPHER_TM_Result
{
	/*
	 * Signals that the action succeeded (i.e. update/delete performed, lock
	 * was acquired)
	 */
	CYPHER_TM_Ok,

	/* The affected tuple wasn't visible to the relevant snapshot */
	CYPHER_TM_Invisible,

	/* The affected tuple was already modified by the calling backend */
	CYPHER_TM_SelfModified,

	/*
	 * The affected tuple was updated by another transaction. This includes
	 * the case where tuple was moved to another partition.
	 */
	CYPHER_TM_Updated,

	/* The affected tuple was deleted by another transaction */
	CYPHER_TM_Deleted,

	/*
	 * The affected tuple is currently being modified by another session. This
	 * will only be returned if table_(update/delete/lock_tuple) are
	 * instructed not to wait.
	 */
	CYPHER_TM_BeingModified,

	/* lock couldn't be acquired, action skipped. Only used by lock_tuple */
	CYPHER_TM_WouldBlock,

	CYPHER_TM_PrevClauseUpdated

} CYPHER_TM_Result;

/* "options" flag bits for heap_insert */
#define HEAP_INSERT_SKIP_FSM	TABLE_INSERT_SKIP_FSM
#define HEAP_INSERT_FROZEN		TABLE_INSERT_FROZEN
#define HEAP_INSERT_NO_LOGICAL	TABLE_INSERT_NO_LOGICAL
#define HEAP_INSERT_SPECULATIVE 0x0010

typedef struct BulkInsertStateData *BulkInsertState;
struct TupleTableSlot;

#define MaxLockTupleMode	LockTupleExclusive

/*
 * Descriptor for heap table scans.
 */
typedef struct CypherHeapScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */

	/* state set up at initscan time */
	BlockNumber rs_nblocks;		/* total number of blocks in rel */
	BlockNumber rs_startblock;	/* block # to start at */
	BlockNumber rs_numblocks;	/* max number of blocks to scan */
	/* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */

	/* scan current state */
	bool		rs_inited;		/* false = scan not init'd yet */
	BlockNumber rs_cblock;		/* current block # in scan, if any */
	Buffer		rs_cbuf;		/* current buffer in scan, if any */
	/* NB: if rs_cbuf is not InvalidBuffer, we hold a pin on that buffer */

	/* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */
	BufferAccessStrategy rs_strategy;	/* access strategy for reads */

	HeapTupleData rs_ctup;		/* current tuple in scan, if any */

	/*
	 * For parallel scans to store page allocation data.  NULL when not
	 * performing a parallel scan.
	 */
	ParallelBlockTableScanWorkerData *rs_parallelworkerdata;

	/* these fields only used in page-at-a-time mode and for bitmap scans */
	int			rs_cindex;		/* current tuple's index in vistuples */
	int			rs_ntuples;		/* number of visible tuples on page */
	OffsetNumber rs_vistuples[MaxHeapTuplesPerPage];	/* their offsets */
}			CypherHeapScanDescData;
typedef struct CypherHeapScanDescData *CypherHeapScanDesc;

/*
 * Descriptor for fetches from heap via an index.
 */
/*
typedef struct IndexFetchHeapData
{
	IndexFetchTableData xs_base;	// AM independent part of the descriptor

	Buffer		xs_cbuf;		// current heap buffer in scan, if any 
	// NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer 
} IndexFetchHeapData;
*/
/* Result codes for HeapTupleSatisfiesVacuum */
/*
typedef enum
{
	HEAPTUPLE_DEAD,				// tuple is dead and deletable 
	HEAPTUPLE_LIVE,				// tuple is live (committed, no deleter)
	HEAPTUPLE_RECENTLY_DEAD,	// tuple is dead, but not deletable yet 
	HEAPTUPLE_INSERT_IN_PROGRESS,	// inserting xact is still in progress
	HEAPTUPLE_DELETE_IN_PROGRESS	// deleting xact is still in progress 
} HTSV_Result;
*/
/* ----------------
 *		function prototypes for heap access method
 *
 * heap_create, heap_create_with_catalog, and heap_drop_with_catalog
 * are declared in catalog/heap.h
 * ----------------
 */


/*
 * HeapScanIsValid
 *		True iff the heap scan is valid.
 */
#define HeapScanIsValid(scan) PointerIsValid(scan)

extern TableScanDesc cypher_heap_beginscan(Relation relation, Snapshot snapshot,
									int nkeys, ScanKey key,
									ParallelTableScanDesc parallel_scan,
									uint32 flags);
extern void cypher_heap_setscanlimits(TableScanDesc scan, BlockNumber startBlk,
							   BlockNumber numBlks);
extern void cypher_heapgetpage(TableScanDesc scan, BlockNumber page);
extern void cypher_heap_rescan(TableScanDesc scan, ScanKey key, bool set_params,
						bool allow_strat, bool allow_sync, bool allow_pagemode);
extern void cypher_heap_endscan(TableScanDesc scan);
extern HeapTuple cypher_heap_getnext(TableScanDesc scan, ScanDirection direction);
extern bool cypher_heap_getnextslot(TableScanDesc sscan,
							 ScanDirection direction, struct TupleTableSlot *slot);
extern void cypher_heap_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
							  ItemPointer maxtid);
extern bool cypher_heap_getnextslot_tidrange(TableScanDesc sscan,
									  ScanDirection direction,
									  TupleTableSlot *slot);
extern bool cypher_heap_fetch(Relation relation, Snapshot snapshot,
					   HeapTuple tuple, Buffer *userbuf);
extern bool cypher_heap_fetch_extended(Relation relation, Snapshot snapshot,
								HeapTuple tuple, Buffer *userbuf,
								bool keep_buf);
extern bool cypher_heap_hot_search_buffer(ItemPointer tid, Relation relation,
								   Buffer buffer, Snapshot snapshot, HeapTuple heapTuple,
								   bool *all_dead, bool first_call);

extern void cypher_heap_get_latest_tid(TableScanDesc scan, ItemPointer tid);

extern BulkInsertState CypherGetBulkInsertState(void);
extern void CypherFreeBulkInsertState(BulkInsertState);
extern void CypherReleaseBulkInsertStatePin(BulkInsertState bistate);

extern void cypher_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
						int options, BulkInsertState bistate);
extern void cypher_heap_multi_insert(Relation relation, struct TupleTableSlot **slots,
							  int ntuples, CommandId cid, int options,
							  BulkInsertState bistate);
extern CYPHER_TM_Result cypher_heap_delete(Relation relation, ItemPointer tid,
							 CommandId cid, Snapshot crosscheck, bool wait,
							 struct TM_FailureData *tmfd, bool changingPart);
extern void cypher_heap_finish_speculative(Relation relation, ItemPointer tid);
extern void cypher_heap_abort_speculative(Relation relation, ItemPointer tid);
extern CYPHER_TM_Result cypher_heap_update(Relation relation, ItemPointer otid,
							 HeapTuple newtup,
							 CommandId cid, Snapshot crosscheck, bool wait,
							 struct TM_FailureData *tmfd, LockTupleMode *lockmode);
extern CYPHER_TM_Result cypher_heap_lock_tuple(Relation relation, HeapTuple tuple,
								 CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
								 bool follow_update,
								 Buffer *buffer, struct TM_FailureData *tmfd);

extern void cypher_heap_inplace_update(Relation relation, HeapTuple tuple);
extern bool cypher_heap_freeze_tuple(HeapTupleHeader tuple,
							  TransactionId relfrozenxid, TransactionId relminmxid,
							  TransactionId cutoff_xid, TransactionId cutoff_multi);
extern bool cypher_heap_tuple_needs_freeze(HeapTupleHeader tuple, TransactionId cutoff_xid,
									MultiXactId cutoff_multi, Buffer buf);
extern bool cypher_heap_tuple_needs_eventual_freeze(HeapTupleHeader tuple);

extern void cypher_simple_heap_insert(Relation relation, HeapTuple tup);
extern void cypher_simple_heap_delete(Relation relation, ItemPointer tid);
extern void cypher_simple_heap_update(Relation relation, ItemPointer otid,
							   HeapTuple tup);

extern TransactionId cypher_heap_index_delete_tuples(Relation rel,
											  TM_IndexDeleteOp *delstate);

/* in heap/pruneheap.c */
struct GlobalVisState;
extern void cypher_heap_page_prune_opt(Relation relation, Buffer buffer);
extern int	cypher_heap_page_prune(Relation relation, Buffer buffer,
							struct GlobalVisState *vistest,
							TransactionId old_snap_xmin,
							TimestampTz old_snap_ts_ts,
							bool report_stats,
							OffsetNumber *off_loc);
extern void cypher_heap_page_prune_execute(Buffer buffer,
									OffsetNumber *redirected, int nredirected,
									OffsetNumber *nowdead, int ndead,
									OffsetNumber *nowunused, int nunused);
extern void cypher_heap_get_root_tuples(Page page, OffsetNumber *root_offsets);

/* in heap/vacuumlazy.c */
struct VacuumParams;
extern void cypher_heap_vacuum_rel(Relation rel,
							struct VacuumParams *params, BufferAccessStrategy bstrategy);
extern void cypher_parallel_vacuum_main(dsm_segment *seg, shm_toc *toc);

/* in heap/heapam_visibility.c */
extern bool CypherHeapTupleSatisfiesVisibility(HeapTuple stup, Snapshot snapshot,
										 Buffer buffer);
extern CYPHER_TM_Result CypherHeapTupleSatisfiesUpdate(HeapTuple stup, CommandId curcid,
										  Buffer buffer);
extern HTSV_Result CypherHeapTupleSatisfiesVacuum(HeapTuple stup, TransactionId OldestXmin,
											Buffer buffer);
extern HTSV_Result CypherHeapTupleSatisfiesVacuumHorizon(HeapTuple stup, Buffer buffer,
												   TransactionId *dead_after);
extern void CypherHeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
								 uint16 infomask, TransactionId xid);
extern bool CypherHeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple);
extern bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);
extern bool CypherHeapTupleIsSurelyDead(HeapTuple htup,
								  struct GlobalVisState *vistest);

/*
 * To avoid leaking too much knowledge about reorderbuffer implementation
 * details this is implemented in reorderbuffer.c not heapam_visibility.c
 */
struct HTAB;
extern bool ResolveCminCmaxDuringDecoding(struct HTAB *tuplecid_data,
										  Snapshot snapshot,
										  HeapTuple htup,
										  Buffer buffer,
										  CommandId *cmin, CommandId *cmax);
extern void CypherHeapCheckForSerializableConflictOut(bool valid, Relation relation, HeapTuple tuple,
												Buffer buffer, Snapshot snapshot);

#endif							/* HEAPAM_H */
