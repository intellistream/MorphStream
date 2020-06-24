package application.bolts.lg;

import application.model.geoip.IPLocation;
import application.model.geoip.IPLocationFactory;
import application.model.geoip.Location;
import application.util.datatypes.StreamValues;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static application.constants.BaseConstants.BaseConf;
import static application.constants.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class GeographyBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeographyBolt.class);
    private static final long serialVersionUID = 8338380760260959675L;

    private IPLocation resolver;

    private double cnt = 0, cnt1 = 0;

    public GeographyBolt() {
        super(LOG);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        String ipResolver = config.getString(BaseConf.GEOIP_INSTANCE);
        resolver = IPLocationFactory.create(ipResolver, config);
        LOG.info(Thread.currentThread().getName());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        cnt++;
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        String ip = in.getStringByField(Field.IP);
        Location location = resolver.resolve(ip);

        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();
//            cnt1++;


            collector.emit(bid, new StreamValues(country, city));

        }
//        double i=(cnt1-cnt)/cnt;
//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {

        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            String ip = in.getStringByField(Field.IP, i);
            Location location = resolver.resolve(ip);

            if (location != null) {
                String city = location.getCity();
                String country = location.getCountryName();

                collector.emit(bid, new StreamValues(country, city));

            }
        }
    }


    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.CITY);
    }
}
