package application.bolts.transactional.tp;


import application.param.lr.LREvent;
import application.sink.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state_engine.DatabaseException;
import state_engine.transaction.impl.TxnContext;


/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_ts_nopush extends TPBolt_ts {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_ts_nopush.class);
    private static final long serialVersionUID = -5968750340131744744L;

    public TPBolt_ts_nopush(int fid, SINKCombo sink) {
        super(fid, sink);
    }

    @Override
    protected void REQUEST_CONSTRUCT(LREvent event, TxnContext txnContext) throws DatabaseException {
        //it simply construct the operations and return.
        transactionManager.Asy_ReadRecord(txnContext
                , "segment_speed"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.speed_value,//holder to be filled up.
                null
        );          //asynchronously return.

        transactionManager.Asy_ReadRecord(txnContext
                , "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.count_value//holder to be filled up.
                , null
        );          //asynchronously return.
        LREvents.add(event);
    }

    @Override
    protected void REQUEST_REQUEST_CORE() {

        for (LREvent event : LREvents) {
            TXN_REQUEST_CORE(event);
        }
    }
}
