package application.util.model.finance;

import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.*;

public class TimeSeries {

    private final HashMap<IndicatorParameter, List<BigDecimal>> previousPriceCache;
    private final HashMap<IndicatorParameter, BigDecimal> smaCache;
    private final HashMap<IndicatorParameter, Double> rocCache;
    private final HashMap<IndicatorParameter, BigDecimal> maxCache;
    private final HashMap<IndicatorParameter, BigDecimal> minCache;
    private TreeMap<DateTime, BigDecimal> prices;

    public TimeSeries(TreeMap<DateTime, BigDecimal> prices) {
        this.prices = prices;
        this.previousPriceCache = new HashMap<>();
        this.smaCache = new HashMap<>();
        this.rocCache = new HashMap<>();
        this.maxCache = new HashMap<>();
        this.minCache = new HashMap<>();
    }

    public BigDecimal openOnDay(DateTime date) {
        DateTime midnight = date.toDateMidnight().toDateTime();
        NavigableMap<DateTime, BigDecimal> previousPrices = prices.subMap(midnight, true, date, true);
        return previousPrices.firstEntry().getValue();
    }

    public DateTime closeOnDay(DateTime date) {
        DateTime midnight = date.toDateMidnight().toDateTime();
        DateTime nextDay = midnight.plusDays(1);
        return prices.floorEntry(nextDay).getKey();
    }

    public BigDecimal priceAt(DateTime date) {
        return prices.get(date);
    }

    public boolean hasPriceAt(DateTime date) {
        return prices.containsKey(date);
    }

    public DateTime beginningOfSeries() {
        return prices.firstKey();
    }

    public DateTime lastOfSeries() {
        return prices.lastKey();
    }

    public SortedMap<DateTime, BigDecimal> dateSlice(DateTime startPoint, DateTime endPoint) {
        return prices.subMap(startPoint, true, endPoint, true);
    }

    public TreeMap<DateTime, BigDecimal> getPrices() {
        return prices;
    }

    void removeDays(int i) {
        DateTime first = beginningOfSeries();
        DateTime midnight = first.withTimeAtStartOfDay();//.toDateMidnight().toDateTime();
        DateTime cutoff = midnight.plusDays(i);

        prices = new TreeMap<>(prices.tailMap(cutoff));
    }
}