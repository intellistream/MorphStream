package intellistream.morphstream.examples.tsp.onlinebiding;

import intellistream.morphstream.examples.tsp.onlinebiding.op.OBBolt_lwm;
import intellistream.morphstream.examples.tsp.onlinebiding.op.OBBolt_olb;
import intellistream.morphstream.examples.tsp.onlinebiding.op.OBBolt_sstore;
import intellistream.morphstream.examples.tsp.onlinebiding.op.OBBolt_ts;
import intellistream.morphstream.examples.tsp.onlinebiding.util.OBInitializer;
import intellistream.morphstream.common.constants.OnlineBidingSystemConstants.Component;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.exception.InvalidIDException;
import intellistream.morphstream.engine.stream.components.grouping.ShuffleGrouping;
import intellistream.morphstream.engine.stream.controller.input.scheduler.SequentialScheduler;
import intellistream.morphstream.engine.stream.topology.delete.TransactionTopology;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.transaction.TableInitilizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.common.constants.OnlineBidingSystemConstants.Conf.OB_THREADS;
import static intellistream.morphstream.common.constants.OnlineBidingSystemConstants.PREFIX;
import static intellistream.morphstream.configuration.CONTROL.enable_app_combo;
import static intellistream.morphstream.util.PartitionHelper.setPartition_interval;

/**
 * Short term as OB.
 */
public class OnlineBiding extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineBiding.class);

    public OnlineBiding(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    //configure set_executor_ready database table.
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        int numberOfStates = config.getInt("NUM_ITEMS");
        setPartition_interval((int) (Math.ceil(numberOfStates / (double) tthread)), tthread);
        TableInitilizer ini = new OBInitializer(db, numberOfStates, theta, tthread, config);
        ini.creates_Table(config);
        if (config.getBoolean("partition", false)) {
            for (int i = 0; i < tthread; i++)
                spinlock_[i] = new SpinLock();
            PartitionedOrderLock.getInstance().initilize(tthread);
        }
        return ini;
    }

    @Override
    public Topology buildTopology() {
//        try {
//            //Spout needs to put two types of events: deposits and transfers
//            //Deposits put values into Accounts and the Book
//            //Transfers atomically move values between accounts and book entries, under a precondition
//            builder.setSpout(Component.SPOUT, spout, spoutThreads);
//            if (enable_app_combo) {
//                //spout only.
//            } else {
//                switch (config.getInt("CCOption", 0)) {
//                    case 1: {//LOB
//                        builder.setBolt(Component.OB, new OBBolt_olb(0)//
//                                , config.getInt(OB_THREADS, 1)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case 2: {//LWM
//                        builder.setBolt(Component.OB, new OBBolt_lwm(0)//
//                                , config.getInt(OB_THREADS, 1)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
////
//                    case 3: {//T-Stream
//                        builder.setBolt(Component.OB, new OBBolt_ts(0)//
//                                , config.getInt(OB_THREADS, 1)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case 4: {//SStore
//                        builder.setBolt(Component.OB, new OBBolt_sstore(0)//
//                                , config.getInt(OB_THREADS, 1)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                }
//                builder.setSink(Component.SINK, sink, sinkThreads
//                        , new ShuffleGrouping(Component.OB)
//                );
//            }
//        } catch (InvalidIDException e) {
//            e.printStackTrace();
//        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology();
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
