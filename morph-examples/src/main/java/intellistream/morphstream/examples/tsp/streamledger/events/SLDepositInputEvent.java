package intellistream.morphstream.examples.tsp.streamledger.events;

import intellistream.morphstream.api.input.InputEvent;

/**
 * Streamledger deposit transaction, which write without dependency.
 */
public class SLDepositInputEvent extends InputEvent {
    private final int accountId;
    private final int assetId;
    private final int accountAmount;
    private final int assetAmount;
    private int id;

    public SLDepositInputEvent(int id, int accountId, int assetId) {
        this.id = id;
        this.accountId = accountId;
        this.assetId = assetId;
        this.accountAmount = 10;
        this.assetAmount = 10;
    }

    public SLDepositInputEvent(int accountId, int assetId, int accountAmount, int assetAmount) {
        this.accountId = accountId;
        this.assetId = assetId;
        this.accountAmount = accountAmount;
        this.assetAmount = assetAmount;
    }

    public int getAccountId() {
        return accountId;
    }

    @Override
    public String toString() {
        return id + "," +
                accountId + "," +
                assetId;
    }

    @Override
    public String toString(int iterationNumber, int totalTransaction) {
        return (id + (iterationNumber * totalTransaction)) + "," +
                (accountId + iterationNumber) + "," +
                (assetId + iterationNumber);
    }
}
