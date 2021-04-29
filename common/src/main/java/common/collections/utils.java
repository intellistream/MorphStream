package common.collections;
import common.helper.DataSource;
import common.helper.Event;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
/**
 * Got datasource, and the corresponding spout executor has the split wrapper..
 * Created by I309939 on 7/29/2016.
 */
public class utils {
    private static final Logger LOG = LoggerFactory.getLogger(utils.class);
    private static ProducerConfig configure() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "SimplePartitioner");
        props.put("request.required.acks", "0");
        ProducerConfig config = new ProducerConfig(props);
        return config;
    }
    public static void main(String[] args) {
        int function = Integer.parseInt(args[1]);
        switch (function) {
            case 1: {//kafka producer..
                int tuple_size = Integer.parseInt(args[2]);
                int skew = Integer.parseInt(args[3]);
                final boolean test = Boolean.parseBoolean(args[4]);
                final boolean verbose = Boolean.parseBoolean(args[5]);
//                LOG.info("Run producer with skew of:" + skew + ", tuple_size:" + tuple_size + ", verbose?" + verbose);
                DataSource dataSource = new DataSource("applications.helper.wrapper.StringStatesWrapper", skew, test, tuple_size, verbose);
                if (test) {
                    final Event event = dataSource.generateEvent();
                    LOG.info("Test output:" + Arrays.toString(event.getEvent().split(Event.split_expression)));
                    return;
                }
                Producer<String, String> producer = new Producer<>(configure());
                if (verbose) {
                    while (true) {
                        final Event event = dataSource.generateEvent(verbose);
                        KeyedMessage<String, String> data = new KeyedMessage<>("mb", event.getKey(), event.getEvent());
//                        producer.send(data);
                    }
                } else {
                    while (true) {
                        final Event event = dataSource.generateEvent();
                        KeyedMessage<String, String> data = new KeyedMessage<>("mb", event.getKey(), event.getEvent());
//                        producer.send(data);
                    }
                }
            }
//            case 2: {//compatibility Brisk.topology killer.
//                LOG.info("Run Storm killer");
//                kill_storm.kill();
//            }
        }
    }
}
