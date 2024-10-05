package worker.rdma;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaDatabaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MorphStreamDatabase extends Thread{
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamDatabase.class);
    private final MorphStreamEnv env = MorphStreamEnv.get();
    private final RdmaDatabaseManager rdmaDatabaseManager;
    public MorphStreamDatabase() throws Exception {
        this.rdmaDatabaseManager = new RdmaDatabaseManager(false, env.configuration());
        LOG.info("Database is initialized");
    }

  public void run() {
      try {
          Thread.sleep(1000);
      } catch (InterruptedException e) {
          throw new RuntimeException(e);
      }
  }
}
