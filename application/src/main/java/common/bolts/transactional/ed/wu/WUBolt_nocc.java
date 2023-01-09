package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import common.param.ed.wu.WUEvent;
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

public class WUBolt_nocc extends WUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(WUBolt_nocc.class);

    public WUBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }

    public WUBolt_nocc(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) throws DatabaseException {}

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {
        nocc_execute(in);
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            WU_TXN_PROCESS((WUEvent) input_event, i, _bid);
        }
    }

    private void WU_TXN_PROCESS(WUEvent input_event, double i, double _bid) throws DatabaseException, InterruptedException {
        TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
//        WORD_UPDATE_REQUEST(input_event, txn_context[(int) (i - _bid)]);
        WORD_UPDATE_REQUEST(input_event, txnContext);
        WORD_UPDATE_REQUEST_CORE(input_event);
    }

}
