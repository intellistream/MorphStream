package intellistream.morphstream.examples.tsp.streamledger.op;

import intellistream.morphstream.examples.tsp.streamledger.events.TransactionEvent;

import static java.util.Objects.requireNonNull;

/**
 * Data type describing the result of a processed transaction.
 * It describes whether the transaction was successful as well as the resulting account balances.
 */
public class TransactionResult {
    private TransactionEvent transaction;
    private boolean success;
    private long newSourceAccountBalance;
    private long newTargetAccountBalance;

    /**
     * Creates a new transaction result.
     *
     * @param transaction             The original transaction input_event.
     * @param success                 True, if the transaction was successful, false if not.
     * @param newSourceAccountBalance The resulting balance of the source account.
     * @param newTargetAccountBalance The resulting balance of the target account.
     */
    public TransactionResult(
            TransactionEvent transaction,
            boolean success,
            long newSourceAccountBalance,
            long newTargetAccountBalance) {
        this.transaction = requireNonNull(transaction);
        this.success = success;
        this.newSourceAccountBalance = newSourceAccountBalance;
        this.newTargetAccountBalance = newTargetAccountBalance;
    }

    public TransactionResult() {
    }

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------
    public TransactionEvent getTransaction() {
        return transaction;
    }

    public void setTransaction(TransactionEvent transaction) {
        this.transaction = transaction;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getNewSourceAccountBalance() {
        return newSourceAccountBalance;
    }

    public void setNewSourceAccountBalance(long newSourceAccountBalance) {
        this.newSourceAccountBalance = newSourceAccountBalance;
    }

    public long getNewTargetAccountBalance() {
        return newTargetAccountBalance;
    }

    public void setNewTargetAccountBalance(long newTargetAccountBalance) {
        this.newTargetAccountBalance = newTargetAccountBalance;
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------
    @Override
    public String toString() {
        return "TransactionResult {"
                + "transaction=" + transaction
                + ", success=" + success
                + ", newSourceAccountBalance=" + newSourceAccountBalance
                + ", newTargetAccountBalance=" + newTargetAccountBalance
                + '}';
    }
}
