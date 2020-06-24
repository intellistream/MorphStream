package application.topology.transactional;


import application.bolts.transactional.gs.*;
import application.constants.GrepSumConstants.Component;
import application.topology.transactional.initializer.MBInitializer;
import state_engine.transaction.TableInitilizer;
import application.util.Configuration;
import sesame.components.Topology;
import sesame.components.exception.InvalidIDException;
import sesame.components.grouping.ShuffleGrouping;
import sesame.controller.input.scheduler.SequentialScheduler;
import sesame.topology.TransactionTopology;
import state_engine.common.PartitionedOrderLock;
import state_engine.common.SpinLock;
import state_engine.profiler.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static application.CONTROL.enable_app_combo;
import static application.constants.GrepSumConstants.Conf.Executor_Threads;
import static application.constants.GrepSumConstants.PREFIX;
import static state_engine.content.Content.*;
import static state_engine.utils.PartitionHelper.setPartition_interval;


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

        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        setPartition_interval((int) (Math.ceil(Metrics.NUM_ITEMS / (double) tthread)), tthread);

        TableInitilizer ini = new MBInitializer(db, scale_factor, theta, tthread, config);

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
