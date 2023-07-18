package benchmark.datagenerator.apps.SL.Transaction;

import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class SLTransferEvent extends Event {
    private int id;
    private int sourceAccountId;
    private int destinationAccountId;
    private int sourceAssetId;
    private int destinationAssetId;
    private int accountTransfer;
    private int bookEntryTransfer;

    public SLTransferEvent(int id, int sourceAccountId, int sourceAssetId, int destinationAccountId, int destinationAssetId) {
        this.id = id;
        this.sourceAccountId = sourceAccountId;
        this.destinationAccountId = destinationAccountId;
        this.sourceAssetId = sourceAssetId;
        this.destinationAssetId = destinationAssetId;
        this.accountTransfer = 100;
        this.bookEntryTransfer = 100;
    }

    public SLTransferEvent(int id, int sourceAccountId, int sourceAssetId, int destinationAccountId, int destinationAssetId,
                           int accountTransfer, int bookEntryTransfer) {
        this.id = id;
        this.sourceAccountId = sourceAccountId;
        this.destinationAccountId = destinationAccountId;
        this.sourceAssetId = sourceAssetId;
        this.destinationAssetId = destinationAssetId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
    }

//    @Override
//    public String toString() {
//        return "[" +
//                sourceAccountId + "," +
//                sourceAssetId + "," +
//                destinationAccountId + "," +
//                destinationAssetId +
//                "],";
//    }

    @Override
    public String toString() {
        return id + "," +
                sourceAccountId + "," +
                sourceAssetId + "," +
                destinationAccountId + "," +
                destinationAssetId + "," +
                accountTransfer + "," +
                bookEntryTransfer;
    }

    @Override
    public String toString(int iterationNumber, int totalTransaction) {
        return (id + (iterationNumber * totalTransaction)) + "," +
                (sourceAccountId + iterationNumber) + "," +
                (sourceAssetId + iterationNumber) + "," +
                (destinationAccountId + iterationNumber) + "," +
                (destinationAssetId + iterationNumber) + "," +
                (accountTransfer) + "," +
                (bookEntryTransfer);
    }
}
