package application.util.model.finance;

import org.joda.time.DateTime;

import java.io.Serializable;
import java.math.BigDecimal;

public class Quote implements Serializable {private static final long serialVersionUID = 22L;
    private String symbol;
    private DateTime openDate;
    private int interval;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private int volume;

    public Quote(String symbol, DateTime openDate, int interval, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, int volume) {
        this.symbol = symbol;
        this.openDate = openDate;
        this.interval = interval;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "Date = " + openDate + ", OHLC = " + open + "/" + high + "/"
                + low + "/" + close + ", Volume = " + volume;
    }

    public DateTime getOpenDate() {
        return openDate;
    }

    public int getInterval() {
        return interval;
    }

    public BigDecimal getOpen() {
        return open;
    }

    public BigDecimal getHigh() {
        return high;
    }

    public BigDecimal getLow() {
        return low;
    }

    public BigDecimal getClose() {
        return close;
    }

    public int getVolume() {
        return volume;
    }

    public double getAverage() {
        return (open.doubleValue() + high.doubleValue() + low.doubleValue() + close.doubleValue()) / 4;
    }

    public String getSymbol() {
        return symbol;
    }
}
