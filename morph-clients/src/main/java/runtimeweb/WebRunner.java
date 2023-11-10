package runtimeweb;

import worker.WebServer;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class WebRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WebServer.class);
    private static final String PATH = MorphStreamEnv.get().configuration().getString("dataPath", "data/jobs");

    public static void main(String[] args) {
        SpringApplication.run(WebRunner.class, args);
    }

    @Override
    public void run() {
        System.out.println("WebRunner is running");
    }
}
