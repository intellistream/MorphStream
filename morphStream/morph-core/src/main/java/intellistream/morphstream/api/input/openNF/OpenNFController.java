package intellistream.morphstream.api.input.openNF;

import intellistream.morphstream.api.input.CacheData;

import java.util.concurrent.*;

// SFC: input source -> VNF1 -> controllerThread1 -> VNF1 -> VNF2 -> controllerThread2 -> VNF2 -> VNF3 -> controllerThread3 -> VNF3 -> output sink
public class OpenNFController {
    private int vnfNum;
    private int vnfParallelism;
    private ExecutorService executorService;
    private ConcurrentHashMap<Integer, BlockingQueue<CacheData>> requestQueues;

    public OpenNFController(int vnfNum, int vnfParallelism) {
        this.vnfNum = vnfNum;
        this.vnfParallelism = vnfParallelism;
        executorService = Executors.newFixedThreadPool(vnfNum);
        requestQueues = new ConcurrentHashMap<>();
    }

    public void addVNF(int vnfId) {
        BlockingQueue<CacheData> REventQueue = new LinkedBlockingQueue<>();
        requestQueues.put(vnfId, REventQueue);
        executorService.submit(new OpenNFControllerThread(vnfId, vnfParallelism, REventQueue));
    }

    public void register_event_to_controller(Integer vnfId, CacheData requestData) {
        requestQueues.get(vnfId).offer(requestData);
    }

    public void shutdown() {
        executorService.shutdown();
    }

}



