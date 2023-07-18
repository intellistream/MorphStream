package intellistream.morphstream.engine.txn.durability.struct.Logging;

import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;

import java.io.Serializable;

public interface LoggingEntry extends Serializable {
    void setVote(MetaTypes.OperationStateType vote);
}
