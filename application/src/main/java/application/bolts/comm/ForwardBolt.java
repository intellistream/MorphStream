package application.bolts.comm;

import application.constants.TrafficMonitoringConstants;
import sesame.components.operators.base.MapBolt;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Created by tony on 5/30/2017.
 */
public class ForwardBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardBolt.class);
    private static final long serialVersionUID = -1944527817896634043L;

    int cnt = 0;
    Random r = new Random();

    private ForwardBolt() {
        super(LOG);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {

        //not in use.
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
//		int bound = in.length;
//		final long bid = in.getBID();
//		for (int i = 0; i < bound; i++) {
//			Message values = in.getMsg(i);
////		values.add(1);
//			collector.emit(bid, values);
//
//		}
        //not in use.
    }


    @Override
    public Fields getDefaultFields() {
        return new Fields(TrafficMonitoringConstants.Field.VEHICLE_ID, TrafficMonitoringConstants.Field.DATE_TIME, TrafficMonitoringConstants.Field.OCCUPIED, TrafficMonitoringConstants.Field.SPEED,
                TrafficMonitoringConstants.Field.BEARING, TrafficMonitoringConstants.Field.LATITUDE, TrafficMonitoringConstants.Field.LONGITUDE, TrafficMonitoringConstants.Field.ROAD_ID);
    }
}
