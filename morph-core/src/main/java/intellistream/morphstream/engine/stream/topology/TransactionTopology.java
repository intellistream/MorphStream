package intellistream.morphstream.engine.stream.topology;

import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.db.CavaliaDatabase;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.transaction.TableInitilizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 */
public abstract class TransactionTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionTopology.class);
    public final transient CavaliaDatabase db;

    protected TransactionTopology(String topologyName, Configuration config) {
        super(topologyName, config);
        assert CONTROL.enable_shared_state;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        if (CONTROL.enable_log) LOG.info(dateFormat.format(date)); //2016/11/16 12:08:43
        this.db = new CavaliaDatabase(config);
    }

    public void initialize() {
        super.initialize();
//        sink = loadSink();
    }

    public abstract TableInitilizer initializeDB(SpinLock[] spinlock);
}
