package intellistream.morphstream.examples.utils.datagen.apps.SL.Transaction;

import intellistream.morphstream.examples.utils.datagen.InputEvent;

/**
 * Streamledger related transaction data
 */
public class SLTransferInputEvent extends InputEvent {
    private final int id;
    private final int sourceAccountId;
    private final int destinationAccountId;
    private final int sourceAssetId;
    private final int destinationAssetId;
    private final int accountTransfer;
    private final int bookEntryTransfer;

    public SLTransferInputEvent(int id, int sourceAccountId, int sourceAssetId, int destinationAccountId, int destinationAssetId) {
        this.id = id;
        this.sourceAccountId = sourceAccountId;
        this.destinationAccountId = destinationAccountId;
        this.sourceAssetId = sourceAssetId;
        this.destinationAssetId = destinationAssetId;
        this.accountTransfer = 100;
        this.bookEntryTransfer = 100;
    }

    public SLTransferInputEvent(int id, int sourceAccountId, int sourceAssetId, int destinationAccountId, int destinationAssetId,
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
