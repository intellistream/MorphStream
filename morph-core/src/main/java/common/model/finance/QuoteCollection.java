package common.model.finance;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class QuoteCollection {
    private static final Logger logger = LoggerFactory.getLogger(QuoteCollection.class.getName());
    private Map<String, TimeSeries> quotes;

    public void QuoteCollection() {
    }

    public Map<String, TimeSeries> getQuotes() {
        return quotes;
    }

    public TimeSeries convertQuoteToTimeSeries(List<Quote> quotes) {
        TreeMap<DateTime, BigDecimal> prices = new TreeMap<>();
        TimeSeries series = new TimeSeries(prices);
        for (Quote quote : quotes) {
            prices.put(quote.getOpenDate(), quote.getOpen());
        }
        return series;
    }

    public void fetchAllQuotes(QuoteFetcher fetcher) throws Exception {
        logger.info("Starting quote fetching");
        Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        String symbolPath = properties.getProperty("symbolPath");
        int days = Integer.parseInt(properties.getProperty("days"));
        int interval = Integer.parseInt(properties.getProperty("interval"));
        Path paths = Paths.get(symbolPath);
        List<String> symbols = Files.readAllLines(paths, StandardCharsets.UTF_8);
        for (String symbol : symbols) {
            symbol = symbol.replaceAll("\\s", "");
            logger.info("Fetching quotes for " + symbol);
            List<Quote> quoteList = fetcher.parseQuotes(symbol, fetcher.fetchQuotes(symbol, days, interval), interval);
            quotes.put(symbol, convertQuoteToTimeSeries(quoteList));
        }
        logger.info("Done fetching quotes");
    }

    public void fetchAllGoogleQuotes() throws Exception {
        fetchAllQuotes(new GoogleQuoteFetcher());
    }
}