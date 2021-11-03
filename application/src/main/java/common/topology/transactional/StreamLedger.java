package common.topology.transactional;

import common.bolts.transactional.sl.SLBolt_lwm;
import common.bolts.transactional.sl.SLBolt_olb;
import common.bolts.transactional.sl.SLBolt_sstore;
import common.bolts.transactional.sl.SLBolt_ts;
import common.collections.Configuration;
import common.constants.StreamLedgerConstants.Component;
import common.topology.transactional.initializer.SLInitializer;
import components.Topology;
import components.exception.InvalidIDException;
import components.grouping.ShuffleGrouping;
import controller.input.scheduler.SequentialScheduler;
import lock.PartitionedOrderLock;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topology.TransactionTopology;
import transaction.TableInitilizer;
import utils.UDF;

import java.util.Random;

import static common.CONTROL.enable_app_combo;
import static common.constants.StreamLedgerConstants.Conf.SL_THREADS;
import static common.constants.StreamLedgerConstants.PREFIX;
import static utils.PartitionHelper.setPartition_interval;

/**
 * Short term as SL.
 */
public class StreamLedger extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StreamLedger.class);

    public StreamLedger(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    static int GenerateInteger(final int min, final int max) {
        Random r = new Random();
        return r.nextInt(max) + min;
    }

    //configure set_executor_ready database table.
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        int numberOfStates = config.getInt("NUM_ITEMS");
        setPartition_interval((int) (Math.ceil(numberOfStates / (double) tthread)), tthread);
        TableInitilizer ini = new SLInitializer(db, config.getString("rootFilePath"), numberOfStates, theta, tthread, config);
        ini.creates_Table(config);
        // initialize UDF
        UDF.complexity = config.getInt("complexity", 100000);
        if (config.getBoolean("partition", false)) {
            for (int i = 0; i < tthread; i++)
                spinlock_[i] = new SpinLock();
//            ini.loadDB(scale_factor, theta, getPartition_interval(), spinlock_);
            //initialize order locks.
            PartitionedOrderLock.getInstance().initilize(tthread);
        } else {
//            ini.loadDB(scale_factor, theta);
        }
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
        return ini;
    }

    @Override
    public Topology buildTopology() {
        try {
            //Spout needs to put two types of events: deposits and transfers
            //Deposits put values into Accounts and the Book
            //Transfers atomically move values between accounts and book entries, under a precondition
            builder.setSpout(Component.SPOUT, spout, spoutThreads);
            if (enable_app_combo) {
                //spout only.
            } else {
                switch (config.getInt("CCOption", 0)) {
                    case 1: {//LOB
                        builder.setBolt(Component.SL, new SLBolt_olb(0)//
                                , config.getInt(SL_THREADS, 1)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case 2: {//LWM
                        builder.setBolt(Component.SL, new SLBolt_lwm(0)//
                                , config.getInt(SL_THREADS, 1)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case 3: {//T-Stream
                        builder.setBolt(Component.SL, new SLBolt_ts(0)//
                                , config.getInt(SL_THREADS, 1)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case 4: {//SStore
                        builder.setBolt(Component.SL, new SLBolt_sstore(0)//
                                , config.getInt(SL_THREADS, 1)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                }
                builder.setSink(Component.SINK, sink, sinkThreads
                        , new ShuffleGrouping(Component.SL)
                );
            }
        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology(db, this);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
