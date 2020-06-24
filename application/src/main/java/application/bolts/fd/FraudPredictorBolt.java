package application.bolts.fd;

import application.constants.BaseConstants;
import application.constants.FraudDetectionConstants;
import application.model.predictor.MarkovModelPredictor;
import application.model.predictor.ModelBasedPredictor;
import application.model.predictor.Prediction;
import application.util.Configuration;
import sesame.components.operators.base.filterBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class FraudPredictorBolt extends filterBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FraudPredictorBolt.class);
    private static final long serialVersionUID = 6445550040247603261L;
    double sel = 0;
    double nsel = 0;
    int cnt = 0;
    int loop = 1;
    private ModelBasedPredictor predictor;


    @Override
    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 65;
        } else {
            return 1;
        }
    }

    public FraudPredictorBolt() {
        super(LOG, new HashMap<>());
        this.read_selectivity = 3;
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 1.0);//workaround to ensure same output selectivity
        this.setStateful();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        String strategy = config.getString(FraudDetectionConstants.Conf.PREDICTOR_MODEL);

        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not in use.
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        final int bound = in.length;
//		final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            char[] entityID = in.getCharArray(0, i);
            char[] record = in.getCharArray(1, i);
//			LOG.info(entityID.length + " " + d_record.length);


            Prediction p = predictor.execute(entityID, record);//use fixed input to stabilize the running?

            // send outliers
//			if (p.isOutlier()) {
//				final StreamValues tuples = new StreamValues(entityID, p.getScore(), StringUtils.join(p.getStates(), ","));
//			collector.emit(0, entityID, p.getScore(), StringUtils.join(p.getStates(), ",").toCharArray());
            collector.emit(0);//a workaround as sink does not need to perform any calculation.
//			sel++;
//			}//else
        }
//		cnt += bound;
    }

    @Override
    public void profile_execute(JumboTuple in) {
        final int bound = in.length;
//		final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            char[] entityID = in.getCharArray(0, i);
            char[] record = in.getCharArray(1, i);
            Prediction p = predictor.execute(entityID, record);//use fixed input to stabilize the running?

            // send outliers
//			if (p.isOutlier()) {
//				final StreamValues tuples = new StreamValues(entityID, p.getScore(), StringUtils.join(p.getStates(), ","));
//			collector.emit(0, entityID, p.getScore(), StringUtils.join(p.getStates(), ",").toCharArray());
            collector.emit_nowait(0);//a workaround as sink does not need to perform any calculation.
//			sel++;
//			}//else
        }
//		cnt += bound;
    }

    public void display() {
//		LOG.info("cnt:" + cnt + "\tcnt1:" + sel + "(" + (sel / cnt) + ")");
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(
                FraudDetectionConstants.Field.ENTITY_ID,
                FraudDetectionConstants.Field.SCORE,
                FraudDetectionConstants.Field.STATES);
    }
}