package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.context.TxnContext;
import transaction.impl.TxnManagerNoLock;

import java.util.Map;

import static common.CONTROL.combo_bid_size;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;

public class TRBolt_nocc extends TRBolt{
    private static final Logger LOG = LoggerFactory.getLogger(TRBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TRBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TRBolt_nocc(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TR_TXN_PROCESS((TREvent) input_event, i, _bid);
        }
    }

    private void TR_TXN_PROCESS(TREvent input_event, double i, double _bid) throws DatabaseException, InterruptedException {
        TWEET_REGISTRANT_REQUEST(input_event, txn_context[(int) (i - _bid)]);
        TWEET_REGISTRANT_REQUEST_CORE(input_event);
    }

}
