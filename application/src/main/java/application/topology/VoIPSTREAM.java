package application.topology;

import application.bolts.comm.GeneralParserBolt;
import application.bolts.vs.*;
import application.util.Configuration;
import sesame.components.Topology;
import sesame.components.exception.InvalidIDException;
import sesame.components.grouping.AllGrouping;
import sesame.components.grouping.FieldsGrouping;
import sesame.components.grouping.GlobalGrouping;
import sesame.components.grouping.ShuffleGrouping;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static application.datatype.util.VSTopologyControl.*;
import static application.constants.VoIPSTREAMConstants.*;
import static application.constants.VoIPSTREAMConstants.Field.*;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoIPSTREAM extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(VoIPSTREAM.class);
    private final int varDetectThreads;
    private final int ecrThreads;
    private final int rcrThreads;
    private final int encrThreads;
    private final int ecr24Threads;
    private final int ct24Threads;
    private final int fofirThreads;
    private final int urlThreads;
    private final int acdThreads;
    private final int scorerThreads;

    public VoIPSTREAM(String topologyName, Configuration config) {
        super(topologyName, config);
        varDetectThreads = config.getInt(Conf.VAR_DETECT_THREADS, 1);
        int vardetectsplitThreads = config.getInt(Conf.VAR_DETECT_Split_THREADS, 1);
        ecrThreads = config.getInt(Conf.ECR_THREADS, 1);
        rcrThreads = config.getInt(Conf.RCR_THREADS, 1);
        encrThreads = config.getInt(Conf.ENCR_THREADS, 1);
        ecr24Threads = config.getInt(Conf.ECR24_THREADS, 1);
        ct24Threads = config.getInt(Conf.CT24_THREADS, 1);
        fofirThreads = config.getInt(Conf.FOFIR_THREADS, 1);
        urlThreads = config.getInt(Conf.URL_THREADS, 1);
        acdThreads = config.getInt(Conf.ACD_THREADS, 1);
        scorerThreads = config.getInt(Conf.SCORER_THREADS, 1);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public Topology buildTopology() {

        try {
            spout.setFields(new Fields(TEXT));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);

            builder.setBolt(Component.PARSER, new GeneralParserBolt(parser,
                            new Fields(CALLING_NUM, CALLED_NUM, ANSWER_TIME, RECORD))
                    , config.getInt(Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.VOICE_DISPATCHER, new VoiceDispatcherBolt(), varDetectThreads,
                    new FieldsGrouping(Component.PARSER
                            , new Fields(CALLING_NUM, CALLED_NUM)
                    ));

            // Filters
            builder.setBolt(Component.RCR, new RCRBolt(), rcrThreads,
                    new FieldsGrouping(Component.VOICE_DISPATCHER
                            , new Fields(CALLING_NUM)
                    ),
                    new FieldsGrouping(Component.VOICE_DISPATCHER, Stream.BACKUP
                            , new Fields(CALLED_NUM)
                    ));
//
            builder.setBolt(Component.ECR, new ECRBolt("ecr"), ecrThreads,
                    new FieldsGrouping(Component.VOICE_DISPATCHER
                            , new Fields(CALLING_NUM)
                    ));

            builder.setBolt(Component.ENCR, new ENCRBolt(), encrThreads,
                    new FieldsGrouping(Component.VOICE_DISPATCHER
                            , new Fields(CALLING_NUM)
                    ));
//

            builder.setBolt(Component.CT24, new CTBolt("ct24"), ct24Threads,
                    new FieldsGrouping(Component.VOICE_DISPATCHER
                            , new Fields(CALLING_NUM)
                    ));

            builder.setBolt(Component.ECR24, new ECRBolt("ecr24"), ecr24Threads,
                    new FieldsGrouping(Component.VOICE_DISPATCHER
                            , new Fields(CALLING_NUM)
                    ));

//
//            // the average must be global, so there must be a partition instance doing that
//            // perhaps a separate bolt, or if multiple bolts are used then a merger should
//            // be employed at the end point.
            builder.setBolt(Component.GLOBAL_ACD, new GlobalACDBolt(), 1,
                    new GlobalGrouping(Component.VOICE_DISPATCHER));

//            // Modules
//
            builder.setBolt(Component.FOFIR, new FoFiRBolt(), fofirThreads,
                    new FieldsGrouping(Component.RCR, RCR_STREAM_ID
                            , new Fields(CALLING_NUM)
                    )
                    ,
                    new FieldsGrouping(Component.ECR, ECR_STREAM_ID
                            , new Fields(CALLING_NUM)
                    )
            );
//
//
            builder.setBolt(Component.URL, new URLBolt(), urlThreads,
                    new FieldsGrouping(Component.ENCR, ENCR_STREAM_ID

                            , new Fields(CALLING_NUM)

                    ),
                    new FieldsGrouping(Component.ECR, ECR_STREAM_ID

                            , new Fields(CALLING_NUM)

                    ));

            builder.setBolt(Component.ACD, new ACDBolt(), acdThreads,
                    new FieldsGrouping(Component.CT24, CTBolt_STREAM_ID

                            , new Fields(CALLING_NUM)

                    ),
                    new FieldsGrouping(Component.ECR24, ECR24_STREAM_ID

                            , new Fields(CALLING_NUM)

                    ),
                    new AllGrouping(Component.GLOBAL_ACD, GlobalACD_STREAM_ID));

//
//            // Score
            builder.setBolt(Component.SCORER, new ScoreBolt(), scorerThreads
                    , new FieldsGrouping(Component.FOFIR, FoFIR_STREAM_ID


                            , new Fields(CALLING_NUM)


                    )
                    , new FieldsGrouping(Component.URL, URL_STREAM_ID

                            , new Fields(CALLING_NUM)
                    )
                    , new FieldsGrouping(Component.ACD, ACD_STREAM_ID
                            , new Fields(CALLING_NUM)
                    )
            );

            builder.setSink(Component.SINK, sink, sinkThreads,
//                    new ShuffleGrouping(Component.ACD));
                    new ShuffleGrouping(Component.SCORER));
        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
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
