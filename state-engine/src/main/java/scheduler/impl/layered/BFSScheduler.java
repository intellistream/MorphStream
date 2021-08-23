package scheduler.impl.layered;

import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.BFSLayeredTPGContext;
import scheduler.struct.bfs.BFSOperation;
import scheduler.struct.bfs.BFSOperationChain;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.List;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class BFSScheduler extends LayeredScheduler<BFSLayeredTPGContext, BFSOperation, BFSOperationChain> {

    public BFSScheduler(int totalThreads, int NUM_ITEMS) {
        super(totalThreads, NUM_ITEMS);
    }

    private void ProcessedToNextLevel(BFSLayeredTPGContext context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
    }

    @Override
    public void EXPLORE(BFSLayeredTPGContext context) {
        BFSOperationChain next = Next(context);
        if (next == null && !context.finished()) {//current level is all processed at the current thread.
            //Check if there's any aborts
            if (context.needAbortHandling) {
                abortHandling(context);
            }
            while (next == null) {
                ProcessedToNextLevel(context);
                next = Next(context);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                //all threads come to the current level.
            }
        }
        DISTRIBUTE(next, context);
    }

    @Override
    protected void NOTIFY(BFSOperationChain operationChain, BFSLayeredTPGContext context) {

    }

    @Override
    public void TxnSubmitFinished(BFSLayeredTPGContext context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<BFSOperation> operationGraph = new ArrayList<>();
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            BFSOperation set_op = null;
            switch (request.accessType) {
                case READ_WRITE_COND: // they can use the same method for processing
                case READ_WRITE:
                    set_op = new BFSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                    set_op = new BFSOperation(getTargetContext(request.d_record), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                    break;
            }
            operationGraph.add(set_op);
            tpg.setupOperationTDFD(set_op, request, context);
        }

        // 4. send operation graph to tpg for tpg construction
//        tpg.setupOperationLD(operationGraph);//TODO: this is bad refactor.
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }
}
