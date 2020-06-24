package application.model.finance;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public abstract class QuoteFetcher implements Serializable {

    private static final long serialVersionUID = 3276841863950349065L;

    abstract public String fetchQuotes(String symbol, int days, int interval) throws Exception;

    abstract public List<Quote> parseQuotes(String symbol, String quoteList, int interval);

    public TimeSeries fetchAndParse(String symbol, int days, int interval) throws Exception {
        String requestResult = fetchQuotes(symbol, days, interval);
        List<Quote> parsed = parseQuotes(symbol, requestResult, interval);

        QuoteCollection qc = new QuoteCollection();

        return qc.convertQuoteToTimeSeries(parsed);
    }

    protected String fetchURLasString(String url) throws IOException, ParseException {
        HttpClient httpclient = HttpClientBuilder.create().build();
        HttpGet httpGet = new HttpGet(url);
        HttpResponse response = httpclient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        String body = EntityUtils.toString(entity);
        EntityUtils.consume(entity);
        //httpGet.releaseConnection();
        return body;
    }

    protected String[] dropLines(String quoteList, int n) {
        String[] lines = quoteList.split("\n");
        lines = Arrays.copyOfRange(lines, n, lines.length);
        return lines;
    }
}