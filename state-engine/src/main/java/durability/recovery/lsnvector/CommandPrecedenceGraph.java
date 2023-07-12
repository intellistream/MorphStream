package durability.recovery.lsnvector;

import durability.struct.Logging.LVCLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.lib.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicInteger;

public class CommandPrecedenceGraph {
    private static final Logger LOG = LoggerFactory.getLogger(CommandPrecedenceGraph.class);
    public int[] GlobalLV;
    public ConcurrentHashMap<Integer, AtomicInteger> GlobalLVMap;
    public ConcurrentHashMap<Integer, Integer> numberOfLocks = new ConcurrentHashMap<>();
    public ConcurrentHashMap<Integer, CSContext> threadToCSContextMap = new ConcurrentHashMap<>();
    public void addContext(int threadId, CSContext context) {
        threadToCSContextMap.put(threadId, context);
    }
    public void addTask(int threadId, LVCLog task) {
        threadToCSContextMap.get(threadId).addTask(task);
    }
    public CSContext getContext(int threadId) {
        return threadToCSContextMap.get(threadId);
    }
    public void init_globalLv(CSContext context) {
        context.totalTaskCount = context.tasks.size();
        if (context.threadId == 0) {
            LOG.info("Start redo");
            if (GlobalLVMap == null) {
                GlobalLVMap = new ConcurrentHashMap<>();
                for (int i = 0; i < threadToCSContextMap.size(); i++) {
                    GlobalLVMap.put(i, new AtomicInteger(0));
                    numberOfLocks.put(i, 0);
                }
            }
        }
    }
    public synchronized void updateGlobalLV(CSContext context) {
        this.GlobalLVMap.get(context.threadId).set(context.readyTask.getLSN() + 1);
    }
    public void reset(CSContext context) {
        context.reset();
        System.out.println(context.threadId + " Number of locks: " + numberOfLocks.get(context.threadId));
    }

    public boolean canEvaluate(LVCLog lvcLog) {
        for (int i = 0; i < GlobalLVMap.size(); i ++) {
            if (lvcLog.getLVs()[i] > GlobalLVMap.get(i).get()) {
                return false;
            }
        }
        return true;
    }

}
