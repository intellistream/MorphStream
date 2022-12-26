package StreamLedger;

import java.util.concurrent.TimeUnit;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

public class LockExamples {
    public static void main(String[] args) throws InterruptedException {
        // connects to 127.0.0.1:6379 by default
        RedissonClient redisson = Redisson.create();

        RLock lock = redisson.getLock("lock");
        lock.lock(2, TimeUnit.SECONDS);

        Thread t = new Thread() {
            public void run() {
                RLock lock1 = redisson.getLock("lock");
                lock1.lock();
                lock1.unlock();
            };
        };

        t.start();
        t.join();

        redisson.shutdown();
    }
}
