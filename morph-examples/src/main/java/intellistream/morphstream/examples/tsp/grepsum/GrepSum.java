package intellistream.morphstream.examples.tsp.grepsum;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.grepsum.op.*;
import intellistream.morphstream.examples.tsp.grepsum.util.GSInitializer;
import intellistream.morphstream.common.constants.GrepSumConstants.Component;
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

import static intellistream.morphstream.common.constants.GrepSumConstants.Conf.Executor_Threads;
import static intellistream.morphstream.common.constants.GrepSumConstants.PREFIX;
import static intellistream.morphstream.configuration.CONTROL.enable_app_combo;
import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.util.PartitionHelper.setPartition_interval;

public class GrepSum extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(GrepSum.class);

    public GrepSum(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    /**
     * Load Data Later by Executors.
     *
     * @param spinlock_
     * @return TableInitilizer
     */
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        int numberOfStates = config.getInt("NUM_ITEMS");
        setPartition_interval((int) (Math.ceil(numberOfStates / (double) tthread)), tthread);
        TableInitilizer ini = new GSInitializer(db, numberOfStates, theta, tthread, config);
        ini.creates_Table(config);
        if (config.getBoolean("partition", false)) {
            for (int i = 0; i < tthread; i++)
                spinlock_[i] = new SpinLock();
            //initilize order locks.
            PartitionedOrderLock.getInstance().initilize(tthread);
        }
        return ini;
    }

    @Override
    public Topology buildTopology() {
//        try {
//            builder.setSpout(Component.SPOUT, new GSCombo(), spoutThreads);
//            if (enable_app_combo) {// enabled by default
//                //spout only.
//            } else { // normal pipelined execution model.
//                switch (config.getInt("CCOption", 0)) {
//                    case CCOption_LOCK: {//no-order
//                        builder.setBolt(Component.EXECUTOR, new GSBolt_nocc(0)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case CCOption_OrderLOCK: {//LOB
//                        builder.setBolt(Component.EXECUTOR, new GSBolt_olb(0)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case CCOption_LWM: {//LWM
//                        builder.setBolt(Component.EXECUTOR, new GSBolt_lwm(0)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case CCOption_MorphStream: {//MorphStream
//                        builder.setBolt(Component.EXECUTOR, new GSBolt_ts(0)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case CCOption_SStore: {//SStore
//                        builder.setBolt(Component.EXECUTOR, new GSBolt_sstore(0)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                }
//                builder.setSink(Component.SINK, new SINKCombo(), sinkThreads
//                        , new ShuffleGrouping(Component.EXECUTOR)
//                );
//            }
//        } catch (InvalidIDException e) {
//            e.printStackTrace();
//        }
//        builder.setGlobalScheduler(new SequentialScheduler());
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
