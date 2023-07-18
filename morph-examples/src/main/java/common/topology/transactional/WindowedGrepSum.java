package common.topology.transactional;

import common.bolts.transactional.gs.*;
import common.collections.Configuration;
import common.constants.GrepSumConstants.Component;
import common.topology.transactional.initializer.GSWInitializer;
import engine.stream.components.Topology;
import engine.stream.components.exception.InvalidIDException;
import engine.stream.components.grouping.ShuffleGrouping;
import engine.stream.controller.input.scheduler.SequentialScheduler;
import engine.txn.lock.PartitionedOrderLock;
import engine.txn.lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.stream.topology.TransactionTopology;
import engine.txn.transaction.TableInitilizer;

import static common.CONTROL.enable_app_combo;
import static common.constants.GrepSumConstants.Conf.Executor_Threads;
import static common.constants.GrepSumConstants.PREFIX;
import static engine.txn.content.Content.*;
import static util.PartitionHelper.setPartition_interval;

public class WindowedGrepSum extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedGrepSum.class);

    public WindowedGrepSum(String topologyName, Configuration config) {
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
        TableInitilizer ini = new GSWInitializer(db, numberOfStates, theta, tthread, config);
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
                        builder.setBolt(Component.EXECUTOR, new GSBolt_nocc(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_OrderLOCK: {//LOB
                        builder.setBolt(Component.EXECUTOR, new GSBolt_olb(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_LWM: {//LWM
                        builder.setBolt(Component.EXECUTOR, new GSBolt_lwm(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_TStream: {//T-Stream
                        builder.setBolt(Component.EXECUTOR, new GSBolt_ts(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(Component.SPOUT));
                        break;
                    }
                    case CCOption_SStore: {//SStore
                        builder.setBolt(Component.EXECUTOR, new GSBolt_sstore(0)//
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
