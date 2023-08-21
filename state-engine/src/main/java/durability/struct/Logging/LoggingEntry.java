package durability.struct.Logging;

import scheduler.struct.MetaTypes;

import java.io.Serializable;

public interface LoggingEntry extends Serializable  {
   void setVote(MetaTypes.OperationStateType vote);
}
