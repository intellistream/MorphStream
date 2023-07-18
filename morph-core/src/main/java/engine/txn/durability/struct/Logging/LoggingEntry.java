package engine.txn.durability.struct.Logging;

import engine.txn.scheduler.struct.MetaTypes;

import java.io.Serializable;

public interface LoggingEntry extends Serializable  {
   void setVote(MetaTypes.OperationStateType vote);
}
