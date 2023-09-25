package intellistream.morphstream.examples.tsp.shj;

import intellistream.morphstream.examples.tsp.shj.op.SHJCombo;
import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.shj.op.*;
import intellistream.morphstream.examples.tsp.shj.util.SHJInitializer;
import intellistream.morphstream.common.constants.SHJConstants.Component;
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

import static intellistream.morphstream.common.constants.SHJConstants.Conf.Executor_Threads;
import static intellistream.morphstream.common.constants.SHJConstants.PREFIX;
import static intellistream.morphstream.configuration.CONTROL.enable_app_combo;
import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.util.PartitionHelper.setPartition_interval;

public class SHJ extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SHJ.class);

    public SHJ(String topologyName, Configuration config) {
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
        TableInitilizer ini = new SHJInitializer(db, numberOfStates, theta, tthread, config);
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
//            builder.setSpout(Component.SPOUT, new SHJCombo(), spoutThreads);
//            if (enable_app_combo) {// enabled by default
//                //spout only.
//            } else { // normal pipelined execution model.
//                switch (config.getInt("CCOption", 0)) {
//                    case CCOption_LOCK: {//no-order
//                        builder.setBolt(Component.EXECUTOR, new SHJBolt_nocc(1)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case CCOption_OrderLOCK: {//LOB
//                        builder.setBolt(Component.EXECUTOR, new SHJBolt_olb(1)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case CCOption_LWM: {//LWM
//                        builder.setBolt(Component.EXECUTOR, new SHJBolt_lwm(1)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case CCOption_MorphStream: {//MorphStream
//                        builder.setBolt(Component.EXECUTOR, new SHJBolt_ts(1)//
//                                , config.getInt(Executor_Threads, 2)
//                                , new ShuffleGrouping(Component.SPOUT));
//                        break;
//                    }
//                    case CCOption_SStore: {//SStore
//                        builder.setBolt(Component.EXECUTOR, new SHJBolt_sstore(1)//
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