package engine.txn.scheduler.signal.recovery;

import engine.txn.durability.recovery.dependency.CommandTask;

public class OnParentLogUpdateSignal {
    public CommandTask commandTask;
    public OnParentLogUpdateSignal(CommandTask commandTask) {
        this.commandTask = commandTask;
    }
}
