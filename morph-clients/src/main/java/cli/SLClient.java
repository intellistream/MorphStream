package cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class SLClient {

    private static final Logger log = LoggerFactory.getLogger(SLClient.class);

    public static void main(String[] args) {
//        System.out.println("Hello World!");
        if (enable_log) log.info("Program Starts..");
        SLClient client = new SLClient();
    }

}
