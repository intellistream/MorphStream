package intellistream.morphstream.examples.tsp.onlinebiding.util;

import intellistream.morphstream.examples.tsp.onlinebiding.events.BuyingEvent;

/**
 * Data type describing the result of a processed transaction.
 * It describes whether the transaction was successful as well as the resulting account balances.
 */
public class BidingResult {
    private BuyingEvent event;
    private boolean success;

    public BidingResult(BuyingEvent event, boolean success) {
        this.success = success;
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------
    @Override
    public String toString() {
        return "TransactionResult {"
                + "input_event=" + event
                + ", success=" + success
                + '}';
    }
}
