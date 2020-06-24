package sesame.components.windowing;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static sesame.components.windowing.EvictionPolicy.Action.*;

public class WindowManager<T> implements TriggerHandler {
    /**
     * Expire old events every EXPIRE_EVENTS_THRESHOLD to
     * keep the window size in measure_end.
     * <p>
     * Note that if the eviction policy is based on watermarks, events will not be evicted until a new
     * watermark would cause them to be considered expired anyway, regardless of this limit
     */
    private static final int EXPIRE_EVENTS_THRESHOLD = 100;
    private static final Logger LOG = LoggerFactory.getLogger(WindowManager.class);
    private final Collection<Event<T>> queue;
    private final WindowLifecycleListener<T> windowLifecycleListener;
    private final AtomicInteger eventsSinceLastExpiry;
    private final List<T> expiredEvents;
    private final Set<Event<T>> prevWindowEvents;
    private final ReentrantLock lock;
    private EvictionPolicy<T, ?> evictionPolicy;
    private TriggerPolicy<T, ?> triggerPolicy;

    public WindowManager(WindowLifecycleListener<T> windowLifecycleListener, Collection<Event<T>> queue) {
        this.queue = queue;
        this.windowLifecycleListener = windowLifecycleListener;
        expiredEvents = new ArrayList<>();
        prevWindowEvents = new HashSet<>();
        eventsSinceLastExpiry = new AtomicInteger();
        lock = new ReentrantLock(true);
    }

    /**
     * The callback_bolt invoked by the trigger policy.
     */
    @Override
    public boolean onTrigger() {
        List<Event<T>> windowEvents = null;
        List<T> expired = null;
        try {
            lock.lock();
            /*
             * scan the entire window to handle out of order events in
             * the case of time based windows.
             */
            windowEvents = scanEvents(true);
            expired = new ArrayList<>(expiredEvents);
            expiredEvents.clear();
        } finally {
            lock.unlock();
        }
        List<T> events = new ArrayList<>();
        List<T> newEvents = new ArrayList<>();
        for (Event<T> event : windowEvents) {
            events.add(event.get());
            if (!prevWindowEvents.contains(event)) {
                newEvents.add(event.get());
            }
        }
        prevWindowEvents.clear();
        if (!events.isEmpty()) {
            prevWindowEvents.addAll(windowEvents);
            //LOG.DEBUG("invoking windowLifecycleListener onActivation, [{}] events in window.", events.size());
            windowLifecycleListener.onActivation(events, newEvents, expired, evictionPolicy.getContext().getReferenceTime());
        } else {
            //LOG.DEBUG("No events in the window, skipping onActivation");
        }
        triggerPolicy.reset();
        return !events.isEmpty();
    }

    public void setEvictionPolicy(EvictionPolicy<T, ?> evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
    }

    public void setTriggerPolicy(TriggerPolicy<T, ?> triggerPolicy) {
        this.triggerPolicy = triggerPolicy;
    }

    public void add(T event) {
        add(event, System.currentTimeMillis());
    }

    /**
     * Add an input_event into the window, with the given ts as the tracking ts.
     *
     * @param event the input_event to track
     * @param ts    the timestamp
     */
    private void add(T event, long ts) {
        add(new BasicEvent<T>(event, ts));
    }

    /**
     * Tracks a window input_event
     *
     * @param windowEvent the window input_event to track
     */
    private void add(Event<T> windowEvent) {
        // watermark events are not added to the queue.
        if (!windowEvent.isWatermark()) {
            queue.add(windowEvent);
        } else {
            //LOG.DEBUG("Got watermark input_event with ts {}", windowEvent.getTimestamp());
        }
        track(windowEvent);
        compactWindow();
    }


    /**
     * feed the input_event to the eviction and trigger policies
     * for bookkeeping and optionally firing the trigger.
     */
    private void track(Event<T> windowEvent) {
        evictionPolicy.track(windowEvent);
        triggerPolicy.track(windowEvent);
    }

    /**
     * expires events that fall out of the window every
     * EXPIRE_EVENTS_THRESHOLD so that the window does not grow
     * too big.
     */
    private void compactWindow() {
        if (eventsSinceLastExpiry.incrementAndGet() >= EXPIRE_EVENTS_THRESHOLD) {
            scanEvents(false);
        }
    }


    /**
     * Scan events in the queue, using the expiration policy to measure_end
     * if the input_event should be evicted or not.
     *
     * @param fullScan if set, will scan the entire queue; if not set, will stop
     *                 as soon as an input_event not satisfying the expiration policy is found
     * @return the list of events to be processed as a part of the current window
     */
    private List<Event<T>> scanEvents(boolean fullScan) {
        //LOG.DEBUG("Scan events, eviction policy {}", evictionPolicy);
        List<T> eventsToExpire = new ArrayList<>();
        List<Event<T>> eventsToProcess = new ArrayList<>();
        try {
            lock.lock();
            Iterator<Event<T>> it = queue.iterator();
            while (it.hasNext()) {
                Event<T> windowEvent = it.next();
                EvictionPolicy.Action action = evictionPolicy.evict(windowEvent);
                if (action == EXPIRE) {
                    eventsToExpire.add(windowEvent.get());
                    it.remove();
                } else if (!fullScan || action == STOP) {
                    break;
                } else if (action == PROCESS) {
                    eventsToProcess.add(windowEvent);
                }
            }
            expiredEvents.addAll(eventsToExpire);
        } finally {
            lock.unlock();
        }
        eventsSinceLastExpiry.set(0);
        //LOG.DEBUG("[{}] events expired from window.", eventsToExpire.size());
        if (!eventsToExpire.isEmpty()) {
            //LOG.DEBUG("invoking windowLifecycleListener.onExpiry");
            windowLifecycleListener.onExpiry(eventsToExpire);
        }
        return eventsToProcess;
    }

    /**
     * Start the trigger policy and waterMarkEventGenerator if set
     */
    protected void start() {
//		if (waterMarkEventGenerator != null) {
//			//LOG.DEBUG("Starting waterMarkEventGenerator");
//			waterMarkEventGenerator.start();
//		}
        //LOG.DEBUG("Starting trigger policy");
        triggerPolicy.start();
    }

    public void shutdown() {
        //LOG.DEBUG("Shutting down WindowManager");
        if (triggerPolicy != null) {
            triggerPolicy.shutdown();
        }
    }

}
