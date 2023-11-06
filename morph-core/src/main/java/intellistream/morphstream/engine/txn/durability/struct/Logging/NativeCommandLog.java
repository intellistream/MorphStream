package intellistream.morphstream.engine.txn.durability.struct.Logging;

import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;

import java.util.ArrayList;

//We use bid instead of LSN
//bid.0 is the first operation in the transaction
//bid.1 is the second operation in the transaction
public class NativeCommandLog extends CommandLog {
    public String id;
    public boolean isAborted = false;

    public NativeCommandLog(long LSN, String tableName, String key, String OperationFunction, String[] conditions, String parameter) {
        super(LSN, tableName, key, OperationFunction, conditions, parameter);
    }

    public static NativeCommandLog getNativeCommandLog(String line) {
        String[] parts = line.split(";");
        String id = parts[0];//id
        String tableName = parts[1];//tableName
        String key = parts[2];//key
        String[] condition = parts[3].split(",");
        ArrayList<String> conditions = new ArrayList<>();
        for (String c : condition) {
            if (c.equals(""))
                continue;
            conditions.add(c);
        }
        String OperationFunction = parts[4];
        String parameter = parts[5];
        NativeCommandLog commandLog = new NativeCommandLog(0, tableName, key, OperationFunction, conditions.toArray(new String[0]), parameter);
        commandLog.setId(id);
        return commandLog;
    }

    @Override
    public void setVote(MetaTypes.OperationStateType vote) {
        if (vote == MetaTypes.OperationStateType.ABORTED)
            isAborted = true;
    }

    public void setId(String txn_id) {
        this.id = txn_id;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(id).append(";");//0-LSN
        stringBuilder.append(tableName).append(";");//1-tableName
        stringBuilder.append(key).append(";");//2-key
        for (String ckeys : condition) {//3-conditions
            stringBuilder.append(ckeys).append(",");
        }
        stringBuilder.append(";");
        stringBuilder.append(OperationFunction).append(";");//4-OperationFunction
        stringBuilder.append(parameter).append(";");//5-parameter
        return stringBuilder.toString();
    }

    @Override
    public int compareTo(CommandLog o) {
        return Double.compare(Double.parseDouble(this.id), Double.parseDouble(((NativeCommandLog) o).id));
    }
}