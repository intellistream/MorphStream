package common.topology.transactional;

import common.bolts.transactional.ibwj.*;
import common.collections.Configuration;
import common.constants.IBWJConstants.Component;
import common.topology.transactional.initializer.IBWJInitializer;
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

import static common.CONTROL.enable_app_combo;
import static common.constants.GrepSumConstants.Conf.Executor_Threads;
import static common.constants.GrepSumConstants.PREFIX;
import static content.Content.*;
import static utils.PartitionHelper.setPartition_interval;

public class IBWJ extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(IBWJ.class);

    public IBWJ(String topologyName, Configuration config) {
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
        TableInitilizer ini = new IBWJInitializer(db, numberOfStates, theta, tthread, config);
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
        try {
            builder.setSpout(Component.SPOUT, spout, spoutThreads);
            if (enable_app_combo) {// enabled by default
                //spout only.
            } else { // normal pipelined execution model.
                switch (config.getInt("CCOption", 0)) {
                    case CCOption_LOCK: {//no-order
                        builder.setBolt(Component.EXECUTOR, new IBWJBolt_nocc(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_OrderLOCK: {//LOB
                        builder.setBolt(Component.EXECUTOR, new IBWJBolt_olb(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_LWM: {//LWM
                        builder.setBolt(Component.EXECUTOR, new IBWJBolt_lwm(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_TStream: {//T-Stream
                        builder.setBolt(Component.EXECUTOR, new IBWJBolt_ts(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_SStore: {//SStore
                        builder.setBolt(Component.EXECUTOR, new IBWJBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                }
                builder.setSink(Component.SINK, sink, sinkThreads
                        , new ShuffleGrouping(Component.EXECUTOR)
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
