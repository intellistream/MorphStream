package intellistream.morphstream.examples.tsp.grepsumnon;

import intellistream.morphstream.examples.tsp.grepsum.op.GSBolt_ts;
import intellistream.morphstream.examples.tsp.grepsumnon.util.NonGSInitializer;
import intellistream.morphstream.common.constants.GrepSumConstants;
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

import static intellistream.morphstream.common.constants.NonGrepSumConstants.Component;
import static intellistream.morphstream.common.constants.NonGrepSumConstants.Conf.Executor_Threads;
import static intellistream.morphstream.common.constants.NonGrepSumConstants.PREFIX;
import static intellistream.morphstream.configuration.CONTROL.enable_app_combo;
import static intellistream.morphstream.configuration.Constants.CCOption_MorphStream;
import static intellistream.morphstream.util.PartitionHelper.setPartition_interval;

public class NonGrepSum extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(NonGrepSum.class);

    public NonGrepSum(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        int numberOfStates = config.getInt("NUM_ITEMS");
        setPartition_interval((int) (Math.ceil(numberOfStates / (double) tthread)), tthread);
        TableInitilizer ini = new NonGSInitializer(db, numberOfStates, theta, tthread, config);
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
//            builder.setSpout(Component.SPOUT, spout, spoutThreads);
//            if (!enable_app_combo) {
//                if (config.getInt("CCOption", 0) == CCOption_MorphStream) {//MorphStream
//                    builder.setBolt(GrepSumConstants.Component.EXECUTOR, new GSBolt_ts(0)//
//                            , config.getInt(Executor_Threads, 2)
//                            , new ShuffleGrouping(GrepSumConstants.Component.SPOUT));
//                }
//                builder.setSink(Component.SINK, sink, sinkThreads
//                        , new ShuffleGrouping(Component.EXECUTOR)
//                );
//            }
//        } catch (InvalidIDException e) {
//            throw new RuntimeException(e);
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