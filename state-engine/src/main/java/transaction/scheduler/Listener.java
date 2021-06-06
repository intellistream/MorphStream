package transaction.scheduler;

import common.OperationChain;

import java.util.ArrayList;

public class Listener implements IOnDependencyResolvedListener{

    private final ArrayList<OperationChain>[] leftOversLocal;
    private final ArrayList<OperationChain>[] withDependentsLocal;

    public Listener(ArrayList<OperationChain>[] leftOversLocal, ArrayList<OperationChain>[] withDependentsLocal) {
        this.leftOversLocal = leftOversLocal;
        this.withDependentsLocal = withDependentsLocal;
    }

    @Override
    public void onDependencyResolvedListener(int threadId, OperationChain oc) {
        if (oc.hasDependents())
            withDependentsLocal[threadId].add(oc);
        else
            leftOversLocal[threadId].add(oc);
    }

}
