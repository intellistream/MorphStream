package application.topology.transactional;


import application.bolts.transactional.sl.SLBolt_lwm;
import application.bolts.transactional.sl.SLBolt_olb;
import application.bolts.transactional.sl.SLBolt_sstore;
import application.bolts.transactional.sl.SLBolt_ts;
import application.constants.StreamLedgerConstants.Component;
import application.topology.transactional.initializer.SLInitializer;
import application.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.Topology;
import sesame.components.exception.InvalidIDException;
import sesame.components.grouping.ShuffleGrouping;
import sesame.controller.input.scheduler.SequentialScheduler;
import sesame.topology.TransactionTopology;
import state_engine.common.PartitionedOrderLock;
import state_engine.common.SpinLock;
import state_engine.transaction.TableInitilizer;

import java.util.Random;

import static application.CONTROL.enable_app_combo;
import static application.constants.StreamLedgerConstants.Conf.SL_THREADS;
import static application.constants.StreamLedgerConstants.Constant.NUM_ACCOUNTS;
import static application.constants.StreamLedgerConstants.PREFIX;
import static state_engine.utils.PartitionHelper.setPartition_interval;

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
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        setPartition_interval((int) (Math.ceil(NUM_ACCOUNTS / (double) tthread)), tthread);
        TableInitilizer ini = new SLInitializer(db, scale_factor, theta, tthread, config);

        ini.creates_Table(config);

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
