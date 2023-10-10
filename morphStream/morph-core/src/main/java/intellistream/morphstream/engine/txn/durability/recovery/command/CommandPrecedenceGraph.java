package intellistream.morphstream.engine.txn.durability.recovery.command;

import intellistream.morphstream.engine.txn.durability.struct.Logging.NativeCommandLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentSkipListSet;

public class CommandPrecedenceGraph {
    private static final Logger LOG = LoggerFactory.getLogger(CommandPrecedenceGraph.class);
    public ConcurrentSkipListSet<NativeCommandLog> tasks = new ConcurrentSkipListSet<>();

    public void addTask(NativeCommandLog task) {
        tasks.add(task);
    }
}
