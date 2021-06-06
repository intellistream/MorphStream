package benchmark.datagenerator;

public class DataTransaction {
    private int id;
    private int sourceAccountId;
    private int destinationAccountId;
    private int sourceAssetId;
    private int destinationAssetId;
    private int sourceAccountAmount;
    private int destinationAccountAmount;
    private int sourceAssetAmount;
    private int destinationAssetAmount;

    public DataTransaction(int id, int sourceAccountId, int sourceAssetId, int destinationAccountId, int destinationAssetId) {
        this.id = id;
        this.sourceAccountId = sourceAccountId;
        this.destinationAccountId = destinationAccountId;
        this.sourceAssetId = sourceAssetId;
        this.destinationAssetId = destinationAssetId;
        this.sourceAccountAmount = 10;
        this.destinationAccountAmount = 10;
        this.sourceAssetAmount = 10;
        this.destinationAssetAmount = 10;
    }

    public DataTransaction(int sourceAccountId, int sourceAssetId, int destinationAccountId, int destinationAssetId, int sourceAccountAmount, int sourceAssetAmount, int destinationAssetAmount, int destinationAccountAmount) {
        this.sourceAccountId = sourceAccountId;
        this.destinationAccountId = destinationAccountId;
        this.sourceAssetId = sourceAssetId;
        this.destinationAssetId = destinationAssetId;
        this.sourceAccountAmount = sourceAccountAmount;
        this.destinationAccountAmount = destinationAccountAmount;
        this.sourceAssetAmount = sourceAssetAmount;
        this.destinationAssetAmount = destinationAssetAmount;
    }

    public int getSourceAccountId() {
        return sourceAccountId;
    }

    public void setSourceAccountId(int sourceAccountId) {
        this.sourceAccountId = sourceAccountId;
    }

    public int getDestinationAccountId() {
        return destinationAccountId;
    }

    public void setDestinationAccountId(int destinationAccountId) {
        this.destinationAccountId = destinationAccountId;
    }

    public int getSourceAssetId() {
        return sourceAssetId;
    }

    public void setSourceAssetId(int sourceAssetId) {
        this.sourceAssetId = sourceAssetId;
    }

    public int getDestinationAssetId() {
        return destinationAssetId;
    }

    public void setDestinationAssetId(int destinationAssetId) {
        this.destinationAssetId = destinationAssetId;
    }

    public int getSourceAccountAmount() {
        return sourceAccountAmount;
    }

    public void setSourceAccountAmount(int sourceAccountAmount) {
        this.sourceAccountAmount = sourceAccountAmount;
    }

    public int getDestinationAccountAmount() {
        return destinationAccountAmount;
    }

    public void setDestinationAccountAmount(int destinationAccountAmount) {
        this.destinationAccountAmount = destinationAccountAmount;
    }

    public int getSourceAssetAmount() {
        return sourceAssetAmount;
    }

    public void setSourceAssetAmount(int sourceAssetAmount) {
        this.sourceAssetAmount = sourceAssetAmount;
    }

    public int getDestinationAssetAmount() {
        return destinationAssetAmount;
    }

    public void setDestinationAssetAmount(int destinationAssetAmount) {
        this.destinationAssetAmount = destinationAssetAmount;
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
                destinationAssetId;
    }

    public String toString(int iterationNumber, int totalTransaction) {
        return (id + (iterationNumber * totalTransaction)) + "," +
                (sourceAccountId + iterationNumber) + "," +
                (sourceAssetId + iterationNumber) + "," +
                (destinationAccountId + iterationNumber) + "," +
                (destinationAssetId + iterationNumber);
    }
}
