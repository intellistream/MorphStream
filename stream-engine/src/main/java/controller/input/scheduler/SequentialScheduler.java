package controller.input.scheduler;

import controller.input.InputStreamController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

import static common.CONTROL.enable_log;

/**
 * Created by shuhaozhang on 17/7/16.
 * every executor owns its own input scheduler.
 */
public class SequentialScheduler extends InputStreamController {
    private static final Logger LOG = LoggerFactory.getLogger(SequentialScheduler.class);
    private static final long serialVersionUID = 5653765958500376011L;
    private final LinkedList<Queue> LQ = new LinkedList<>();
    /**
     * non-blocking fetch
     *
     * @return
     */
    private int size;
    private int current = 0;

    public void initialize() {
        super.initialize();
        for (String streamId : keySet) {
            LQ.addAll(getRQ().get(streamId).values());
        }
        size = LQ.size();
        current = 0;
        if (size == 0) {
            if (enable_log) LOG.info("MyQueue initialize wrong");
            System.exit(-1);
        }
    }

    //
//	@Override
//	public JumboTuple fetchResults() {
//		JumboTuple tuple = null;
//		int cnt = 0;
//		do {
//			if (current == size) {
//				current = 0;
//			}
//			tuple = fetchFromqueue(LQ.GetAndUpdate(current++));
//			cnt++;
//		} while (tuple == null && cnt < size);
//
//		return tuple;//return a tuple or null after failed trying all queues.
//	}
    @Override
    public Object fetchResults() {
        if (current == size) {
            current = 0;
        }
//        if (LQ.size() == 1) {
//            System.out.println("LQ size is greater than 1.");
//        }
        return fetchFromqueue(LQ.get(current++));
    }

    @Override
    public Object fetchResultsIndex(int index) {
        if (current == size) {
            current = 0;
        }
        return fetchFromQueue(LQ.get(current++), index);
    }
}
