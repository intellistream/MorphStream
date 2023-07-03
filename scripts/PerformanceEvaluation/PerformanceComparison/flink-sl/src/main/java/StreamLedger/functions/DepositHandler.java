package StreamLedger.functions;

import StreamLedger.data.DepositEvent;
import org.apache.flink.api.common.functions.RichMapFunction;

public class DepositHandler extends RichMapFunction<DepositEvent, Boolean> {
    @Override
    public Boolean map(DepositEvent value) throws Exception {
        return null;
    }
}
