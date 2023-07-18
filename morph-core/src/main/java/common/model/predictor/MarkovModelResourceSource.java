package common.model.predictor;

import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import static common.CONTROL.enable_log;

/**
 * @author mayconbordin
 */
public class MarkovModelResourceSource implements IMarkovModelSource {
    private static final Logger LOG = LoggerFactory.getLogger(MarkovModelResourceSource.class);
    private final Charset charset;

    public MarkovModelResourceSource() {
        charset = Charset.defaultCharset();
    }

    @Override
    public String getModel(String key) {
        try {
            URL url = Resources.getResource(key);
            return Resources.toString(url, charset);
        } catch (IOException ex) {
            if (enable_log) LOG.error("Unable to load markov model from resource " + key, ex);
            return null;
        }
    }
}
