package state_engine.transaction.dedicated.ordered;

import java.util.concurrent.ConcurrentSkipListSet;

public class MyList<O> extends ConcurrentSkipListSet<O> {


    private final String table_name;
    private final String primaryKey;

    public MyList(String table_name, String primaryKey) {
        this.table_name = table_name;
        this.primaryKey = primaryKey;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getTable_name() {
        return table_name;
    }
}
