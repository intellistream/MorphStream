package runtimeweb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class WebRunner implements Runnable {
    public static void main(String[] args) {
        SpringApplication.run(WebRunner.class, args);
    }

    @Override
    public void run() {
        System.out.println("WebRunner is running");
    }
}
