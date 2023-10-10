package StreamLedger.functions;

import StreamLedger.data.TransactionEvent;
import StreamLedger.data.TransactionResult;
import org.apache.flink.api.common.functions.RichMapFunction;

public class TransferHandler extends RichMapFunction<TransactionEvent, TransactionResult> {

    @Override
    public TransactionResult map(TransactionEvent value) throws Exception {
        return null;
    }
}
