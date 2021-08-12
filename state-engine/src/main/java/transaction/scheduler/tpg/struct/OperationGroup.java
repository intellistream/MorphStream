package transaction.scheduler.tpg.struct;

import transaction.scheduler.tpg.TPGContext;
import transaction.scheduler.tpg.struct.MetaTypes.DependencyType;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * operation group will only have one fd parent and one ld parent, which is from the first operation in the group
 * the intuition is to batch a consecutive set of operations following TD that only have TD dependency, the only other dependencies are from the very first operation.
 */
public class OperationGroup {
    private final String operationGroupId;
    private final List<Operation> operationList;

    private final AtomicInteger fd_parents_count;
    private final AtomicInteger ld_parents_count;
    private final AtomicInteger fd_children_count; // TODO: should be deleted after code clean
    private final AtomicInteger ld_children_count;

    // OperationChainKey -> OperationChain
    private final List<Operation> fd_parents; // functional dependent operation chains
    private final List<Operation> ld_parents; // logical dependent operation chains
    private final List<Operation> fd_children; // functional dependent operation chains
    private final List<Operation> ld_children; // logical dependent operation chains

    private OperationGroup td_parent; // td will only have one parent og and one child og
    private OperationGroup td_child;

    public TPGContext context = null;
    public boolean isExecuted = false;

    public OperationGroup(String operationGroupId) {
        this.operationGroupId = operationGroupId;
        this.operationList = new ArrayList<>();

        this.fd_parents_count = new AtomicInteger(0);
        this.ld_parents_count = new AtomicInteger(0);
        this.fd_children_count = new AtomicInteger(0);
        this.ld_children_count = new AtomicInteger(0);

        this.fd_parents = new ArrayList<>();
        this.ld_parents = new ArrayList<>();
        this.fd_children = new ArrayList<>();
        this.ld_children = new ArrayList<>();
    }

    public void addOperation(Operation operation) {
        operationList.add(operation);
        // add countdown for scheduling
        operation.setOG(this);
        if (operationList.size() == 1) { // this is the header of the operation group, try to find out the other types of dependencies
            setContext(operation.context);
            setupDependencies(operation);
        }
    }

    public List<Operation> getOperations() {
        return operationList;
    }

    public String getOperationGroupId() {
        return operationGroupId;
    }

    public void setOGTDParent(OperationGroup operationGroup) {
        setTd_parent(operationGroup);
    }

    public void setOGTDChild(OperationGroup operationGroup) {
        setTd_child(operationGroup);
    }

    public boolean hasChildren() {
        return getFd_children_count().get() != 0 || getLd_children_count().get() != 0;
    }
    public boolean hasParents() {
        return getFd_parents_count().get() != 0 || getLd_parents_count().get() != 0;
    }

    /**
     *
     * @param op the operation to find the XX_parents
     */
    public void setupDependencies(Operation op) {
        // addOperation dependent OCs found from op.
        Queue<Operation> parents = op.getParents(DependencyType.FD);
        fd_parents.addAll(parents);
        fd_parents_count.addAndGet(parents.size());
        parents = op.getParents(DependencyType.LD);
        ld_parents.addAll(parents);
        ld_parents_count.addAndGet(parents.size());
        Queue<Operation> children = op.getChildren(DependencyType.FD);
        fd_children.addAll(children);
        fd_children_count.addAndGet(children.size());
        children = op.getChildren(DependencyType.LD);
        ld_children.addAll(children);
        ld_children_count.addAndGet(children.size());
    }

//    public void setFDParent(OperationGroup parentOG) {
//        OperationGroup ret = fd_parents.putIfAbsent(parentOG.getOperationGroupId(), parentOG);
//        if (ret == null)
//            fd_parents_count.incrementAndGet();
//    }
//
//    public void setLDParent(OperationGroup parentOG) {
//        OperationGroup ret = ld_parents.putIfAbsent(parentOG.getOperationGroupId(), parentOG);
//        if (ret == null)
//            ld_parents_count.incrementAndGet();
//    }
//
//    public void setFDChild(OperationGroup parentOG) {
//        OperationGroup ret = fd_parents.putIfAbsent(parentOG.getOperationGroupId(), parentOG);
//        if (ret == null)
//            fd_parents_count.incrementAndGet();
//    }
//
//    public void setLDChild(OperationGroup parentOG) {
//        OperationGroup ret = ld_parents.putIfAbsent(parentOG.getOperationGroupId(), parentOG);
//        if (ret == null)
//            ld_parents_count.incrementAndGet();
//    }

    public void updateDependencies(DependencyType dependencyType) {
        switch (dependencyType) {
            case FD: {
                getFd_parents_count().decrementAndGet();
                break;
            }
            case LD: {
                getLd_parents_count().decrementAndGet();
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + dependencyType);
        }
    }

    private void setContext(TPGContext context) {
        if (this.context == null) {
            this.context = context;
        }
    }

    @Override
    public String toString() {
        return "OperationGroup{" +
                "operationGroupId='" + operationGroupId + '}';
    }

    public AtomicInteger getFd_parents_count() {
        return fd_parents_count;
    }

    public AtomicInteger getLd_parents_count() {
        return ld_parents_count;
    }

    public AtomicInteger getFd_children_count() {
        return fd_children_count;
    }

    public AtomicInteger getLd_children_count() {
        return ld_children_count;
    }

    public List<Operation> getFd_parents() {
        return fd_parents;
    }

    public List<Operation> getLd_parents() {
        return ld_parents;
    }

    public List<Operation> getFd_children() {
        return fd_children;
    }

    public List<Operation> getLd_children() {
        return ld_children;
    }

    public OperationGroup getTd_parent() {
        return td_parent;
    }

    public void setTd_parent(OperationGroup td_parent) {
        this.td_parent = td_parent;
    }

    public OperationGroup getTd_child() {
        return td_child;
    }

    public void setTd_child(OperationGroup td_child) {
        this.td_child = td_child;
    }
}
