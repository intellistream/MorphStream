package common.topology.transactional;

import common.bolts.transactional.eds.cus.CUSBolt_ts;
import common.bolts.transactional.eds.ess.ESSBolt_ts;
import common.bolts.transactional.eds.scs.SCSBolt_ts;
import common.bolts.transactional.eds.tcgs.TCGSBolt_ts;
import common.bolts.transactional.eds.tcs.TCSBolt_ts;
import common.bolts.transactional.eds.trs.TRSBolt_ts;
import common.bolts.transactional.eds.wus.WUSBolt_ts;
import common.collections.Configuration;
import common.constants.EventDetectionSlidingConstants;
import common.topology.transactional.initializer.EDSInitializer;
import components.Topology;
import components.exception.InvalidIDException;
import components.grouping.ShuffleGrouping;
import controller.input.scheduler.SequentialScheduler;
import execution.runtime.tuple.impl.Fields;
import lock.PartitionedOrderLock;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topology.TransactionTopology;
import transaction.TableInitilizer;

import static content.Content.*;
import static utils.PartitionHelper.setPartition_interval;

public class EventDetectionSliding extends TransactionTopology {

    private static final Logger LOG = LoggerFactory.getLogger(EventDetectionSliding.class);

    public EventDetectionSliding(String topologyName, Configuration config) {
        super(topologyName, config);
    }

//    public void initialize() {
//        super.initialize();
//        sink = loadSink();
//    }

    /**
     * Load Data Later by Executors.
     *
     * @param spinlock_
     * @return TableInitilizer
     */
    @Override
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        int numberOfStates = config.getInt("NUM_ITEMS");
        setPartition_interval((int) (Math.ceil(numberOfStates / (double) tthread)), tthread);
        TableInitilizer ini = new EDSInitializer(db, numberOfStates, theta, tthread, config);
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
            spout.setFields(new Fields(EventDetectionSlidingConstants.Field.TEXT));//output of a spouts
            builder.setSpout(EventDetectionSlidingConstants.Component.SPOUT, spout, spoutThreads); //TODO: Change the constant to FileSpout

            switch (config.getInt("CCOption", 0)) {

                case CCOption_TStream: {//T-Stream
                    builder.setBolt(EventDetectionSlidingConstants.Component.TRS, new TRSBolt_ts(1)//Spout has a fid of zero.
                            , config.getInt(EventDetectionSlidingConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionSlidingConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionSlidingConstants.Component.WUS, new WUSBolt_ts(2)
                            , config.getInt(EventDetectionSlidingConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionSlidingConstants.Component.TRS));
                    builder.setBolt(EventDetectionSlidingConstants.Component.TCS, new TCSBolt_ts(3)
                            , config.getInt(EventDetectionSlidingConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionSlidingConstants.Component.WUS));
                    builder.setBolt(EventDetectionSlidingConstants.Component.TCGS, new TCGSBolt_ts(4)
                            , config.getInt(EventDetectionSlidingConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionSlidingConstants.Component.TCS));
                    builder.setBolt(EventDetectionSlidingConstants.Component.SCS, new SCSBolt_ts(5)
                            , config.getInt(EventDetectionSlidingConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionSlidingConstants.Component.TCGS));
                    builder.setBolt(EventDetectionSlidingConstants.Component.CUS, new CUSBolt_ts(6)
                            , config.getInt(EventDetectionSlidingConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionSlidingConstants.Component.SCS));
                    builder.setBolt(EventDetectionSlidingConstants.Component.ESS, new ESSBolt_ts(7)
                            , config.getInt(EventDetectionSlidingConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionSlidingConstants.Component.CUS));
                    break;
                }

            }
            builder.setSink(EventDetectionSlidingConstants.Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(EventDetectionSlidingConstants.Component.ESS)
            );

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
        return common.constants.EventDetectionSlidingConstants.PREFIX;
    }

}
