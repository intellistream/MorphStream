package scheduler.signal.recovery;

import durability.recovery.dependency.CommandTask;

public class OnParentLogUpdateSignal {
    public CommandTask commandTask;
    public OnParentLogUpdateSignal(CommandTask commandTask) {
        this.commandTask = commandTask;
    }
}
