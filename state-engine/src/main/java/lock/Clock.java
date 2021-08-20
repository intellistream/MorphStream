package lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Centralized clocking system.
 */
public class Clock implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Clock.class);
    long create_time;
    long gap;
    private int iteration = -100;//let it runs for a while...
    private Timer timer;

    public Clock(double checkpoint_interval) {
        gap = (long) (checkpoint_interval * (long) 1E3);//checkpoint_interval=0.1 -- 100ms by default.
        LOG.info("Clock advance interval:" + checkpoint_interval);
        create_time = System.nanoTime();
        timer = new Timer();
    }

    public void start() {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                iteration++;
//				LOG.info("Advance iteration" + iteration + " @" + DateTime.now());
            }
        }, 2 * gap, gap);
    }

    public synchronized boolean tick(int myiteration) {
        //            if (iteration - myiteration > 2) {
        //                LOG.info("System cannot tolerate current spout speed, gaps at " + (iteration - myiteration) * gap / 1E3 + " s, finished measurement (k events/s) -1");
        //                System.exit(-1);
        //            }
        //			final long call_time = System.nanoTime();
        //			return (call_time - create_time) > gap * iteration;
        return myiteration <= iteration;
    }

    @Override
    public void close() {
        timer.cancel();
        timer = null;
    }
}