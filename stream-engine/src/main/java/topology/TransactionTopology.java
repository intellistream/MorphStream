package topology;

import common.collections.Configuration;
import db.CavaliaDatabase;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.TableInitilizer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_shared_state;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 */
public abstract class TransactionTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionTopology.class);
    public final transient CavaliaDatabase db;

    protected TransactionTopology(String topologyName, Configuration config) {
        super(topologyName, config);
        assert enable_shared_state;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        if(enable_log) if(enable_log) LOG.info(dateFormat.format(date)); //2016/11/16 12:08:43
        this.db = new CavaliaDatabase(config.getString("metrics.output") + dateFormat.format(date));
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    public abstract TableInitilizer initializeDB(SpinLock[] spinlock);
}
