package intellistream.morphstream.common.io.Rdma.RdmaUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorsServiceContext {
    private static ExecutorService executorService=null;

    public static ExecutorService getInstance(){
        if(executorService==null){
            synchronized (ExecutorsServiceContext.class){
                if(executorService==null)
                    executorService= Executors.newCachedThreadPool();
            }
        }
        return executorService;
    }
}
