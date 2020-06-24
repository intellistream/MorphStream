package application.bolts.lg;

import application.util.datatypes.StreamValues;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static application.constants.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class GeoStatsBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeoStatsBolt.class);
    private static final long serialVersionUID = -6679894136065066362L;

    private Map<String, CountryStats> stats;

    public GeoStatsBolt() {
        super(LOG, 0.38095238095238095238095238095238);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        stats = new HashMap<>();
        LOG.info(Thread.currentThread().getName());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        if (stat != null) stat.start_measure();
        String country = (String) in.getValue(0);
        String city = (String) in.getValue(1);
        final long bid = in.getBID();
        if (!stats.containsKey(country)) {
            stats.put(country, new CountryStats(country));
        }

        stats.get(country).cityFound(city);

        collector.emit(bid, new StreamValues(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(city)));

//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {

            String country = (String) in.getValue(0, i);
            String city = (String) in.getValue(1, i);
            if (!stats.containsKey(country)) {
                stats.put(country, new CountryStats(country));
            }
            stats.get(country).cityFound(city);
            collector.emit(bid, new StreamValues(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(city)));

        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.COUNTRY_TOTAL, Field.CITY, Field.CITY_TOTAL);
    }

    private class CountryStats {
        private static final int COUNT_INDEX = 0;
        private static final int PERCENTAGE_INDEX = 1;
        private final String countryName;
        private final Map<String, List<Integer>> cityStats = new HashMap<>();
        private int countryTotal = 0;

        CountryStats(String countryName) {
            this.countryName = countryName;
        }

        void cityFound(String cityName) {
            countryTotal++;

            if (cityStats.containsKey(cityName)) {
                cityStats.get(cityName).set(COUNT_INDEX, cityStats.get(cityName).get(COUNT_INDEX) + 1);
            } else {
                List<Integer> list = new LinkedList<>();
                list.add(1);
                list.add(0);
                cityStats.put(cityName, list);
            }

            double percent = (double) cityStats.get(cityName).get(COUNT_INDEX) / (double) countryTotal;
            cityStats.get(cityName).set(PERCENTAGE_INDEX, (int) percent);
        }

        int getCountryTotal() {
            return countryTotal;
        }

        int getCityTotal(String cityName) {
            return cityStats.get(cityName).get(COUNT_INDEX);
        }

        @Override
        public String toString() {
            return "Total Count for " + countryName + " is " + Integer.toString(countryTotal) + "\n"
                    + "Cities: " + cityStats.toString();
        }
    }
}
