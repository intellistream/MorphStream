package transaction.scheduler.picker;

import common.OperationChain;

public abstract class Picker {

    /**
     * Implement the common pickUp methods here.
     * @param threadId
     */
    public OperationChain pickUp(int threadId){
       return null;
    }


    public void reset() {

    }

}
