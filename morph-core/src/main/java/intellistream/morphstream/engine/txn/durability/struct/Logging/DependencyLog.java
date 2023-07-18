package intellistream.morphstream.engine.txn.durability.struct.Logging;

import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;

import java.util.ArrayList;
import java.util.List;

//We use bid instead of LSN
//bid.0 is the first operation in the transaction
//bid.1 is the second operation in the transaction
public class DependencyLog extends CommandLog {
    public String id;
    public boolean isAborted = false;
    List<String> inEdges = new ArrayList<>();
    List<String> outEdges = new ArrayList<>();

    public DependencyLog(long LSN, String tableName, String key, String OperationFunction, String[] conditions, String parameter) {
        super(LSN, tableName, key, OperationFunction, conditions, parameter);
    }

    public DependencyLog(String id, String tableName, String key, List<String> inEdges, List<String> outEdges, String[] conditions, String operationFunction, String para, int isAborted) {
        super(0, tableName, key, operationFunction, conditions, para);
        this.inEdges = inEdges;
        this.outEdges = outEdges;
        this.id = id;
        this.isAborted = isAborted == 1;
    }

    public static DependencyLog getDependencyFromString(String string) {
        String[] strings = string.split(";");
        String id = strings[0];
        String inEdgesString = strings[1];
        List<String> inEdges = new ArrayList<>();
        for (String in : inEdgesString.split(",")) {
            if (in.equals(""))
                continue;
            inEdges.add(in);
        }
        String outEdgesString = strings[2];
        List<String> outEdges = new ArrayList<>();
        for (String out : outEdgesString.split(",")) {
            if (out.equals(""))
                continue;
            outEdges.add(out);
        }
        String tableName = strings[3];
        String key = strings[4];
        String[] condition = strings[5].split(",");
        ArrayList<String> conditions = new ArrayList<>();
        for (String c : condition) {
            if (c.equals(""))
                continue;
            conditions.add(c);
        }
        String operationFunction = strings[6];
        String parameter = strings[7];
        int isAborted = Integer.parseInt(strings[8]);
        return new DependencyLog(id, tableName, key, inEdges, outEdges, conditions.toArray(new String[0]), operationFunction, parameter, isAborted);
    }

    public void setId(String txn_id) {
        this.id = txn_id;
    }

    public void addInEdge(String bid) {
        inEdges.add(bid);
    }

    public void addOutEdge(String bid) {
        outEdges.add(bid);
    }

    public List<String> getInEdges() {
        return inEdges;
    }

    public List<String> getOutEdges() {
        return outEdges;
    }

    public boolean isRoot() {
        return inEdges.size() == 0;
    }

    public boolean isLeaf() {
        return outEdges.size() == 0;
    }

    @Override
    public void setVote(MetaTypes.OperationStateType vote) {
        if (vote == MetaTypes.OperationStateType.ABORTED)
            isAborted = true;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(id).append(";");//LSN
        for (String id : inEdges) {
            stringBuilder.append(id).append(",");
        }
        stringBuilder.append(";");
        for (String id : outEdges) {
            stringBuilder.append(id).append(",");
        }
        stringBuilder.append(";");
        stringBuilder.append(tableName).append(";");
        stringBuilder.append(key).append(";");
        for (String ckeys : condition) {
            stringBuilder.append(ckeys).append(",");
        }
        stringBuilder.append(";");
        stringBuilder.append(OperationFunction).append(";");
        stringBuilder.append(parameter).append(";");
        if (isAborted) {
            stringBuilder.append(1).append(";");
        } else {
            stringBuilder.append(0).append(";");
        }
        return stringBuilder.toString();
    }

    @Override
    public int compareTo(CommandLog o) {
        return Double.compare(Double.parseDouble(this.id), Double.parseDouble(((DependencyLog) o).id));
    }
}
