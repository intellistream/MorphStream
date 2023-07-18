package engine.stream.queue;

import engine.stream.execution.ExecutionNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Queue;

public abstract class QueueController implements Serializable {
    private static final long serialVersionUID = 12L;
    private static final Logger LOG = LoggerFactory.getLogger(QueueController.class);
    final HashMap<Integer, ExecutionNode> downExecutor_list;

    QueueController(HashMap<Integer, ExecutionNode> downExecutor_list) {
        this.downExecutor_list = downExecutor_list;
    }

    public abstract Queue get_queue(int executor);

    public abstract void allocate_queue(boolean linked, int desired_elements_epoch_per_core);

    public abstract boolean isEmpty();
}
