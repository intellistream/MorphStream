package transaction;

import scheduler.impl.Scheduler;

/**
 * Every thread has its own TxnManager.
 */
public abstract class TxnManager implements ITxnManager {
    protected Scheduler scheduler;
}
