package intellistream.morphstream.engine.txn.scheduler.impl.ds;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;
import intellistream.morphstream.engine.db.storage.datatype.StringDataBox;
import intellistream.morphstream.engine.db.storage.impl.RemoteStorageManager;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.ds.RLContext;
import intellistream.morphstream.engine.txn.scheduler.impl.RemoteStorageScheduler;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import intellistream.morphstream.util.AppConfig;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RLScheduler<Context extends RLContext> extends RemoteStorageScheduler<Context> {
    private static final Logger LOG = LoggerFactory.getLogger(RLScheduler.class);
    public final RdmaWorkerManager rdmaWorkerManager;
    public final RemoteStorageManager remoteStorageManager;
    @Getter
    private final ConcurrentHashMap<Integer, Context> threadToContext = new ConcurrentHashMap<>();//threadId -> context
    public static final Client clientObj;
    static {
        try {
            Class<?> clientClass = Class.forName(MorphStreamEnv.get().configuration().getString("clientClassName"));
            clientObj = (Client) clientClass.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    public RLScheduler(RdmaWorkerManager rdmaWorkerManager, RemoteStorageManager remoteStorageManager) {
        this.rdmaWorkerManager = rdmaWorkerManager;
        this.remoteStorageManager = remoteStorageManager;
    }

    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.push(request);
        return true;
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        context.clear();
    }

    @Override
    public void TxnSubmitFinished(Context context, int batchID) {
        //Get Lock
        boolean lock;
        do {
            releaseLock(context);
            lock = getLock(context);
        } while (!lock);
        //Execution
        asyncRead(context);
        while(context.hasUnExecuted()) {
            execute(context);
        }
        //Commit
        if (context.canCommit()) {
            commit(context);
        }
        //Release Lock
        releaseLock(context);
    }
    private boolean getLock(Context context) {
        try {
            for (Operation operation : context.tempOperations) {
                if (operation.accessType == CommonMetaTypes.AccessType.WRITE && !context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                    while (!context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                        boolean isAbort = this.remoteStorageManager.exclusiveLockAcquisition(operation.bid, operation.table_name, operation.pKey,this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
                        if (isAbort) {
                            return false;
                        }
                    }
                } else if (operation.accessType == CommonMetaTypes.AccessType.READ && !context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                    this.remoteStorageManager.sharedLockAcquisition(operation.table_name, operation.pKey,this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
                }
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private void asyncRead(Context context) {
        for (Operation operation : context.tempOperations) {
            try {
                if (operation.accessType == CommonMetaTypes.AccessType.READ) {
                    boolean success = this.remoteStorageManager.syncReadRemoteDatabaseWithSharedLock(operation.table_name, operation.pKey, this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
                    while (!success) {
                        success = this.remoteStorageManager.syncReadRemoteDatabaseWithSharedLock(operation.table_name, operation.pKey, this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
                    }
                } else if (operation.accessType == CommonMetaTypes.AccessType.WRITE) {
                    this.remoteStorageManager.asyncReadRemoteDatabaseWithExclusiveLock(operation.table_name, operation.pKey, this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    private void execute(Context context) {
        for (Operation operation : context.tempOperations) {
            if (operation.operationType == MetaTypes.OperationStateType.ABORTED || operation.operationType == MetaTypes.OperationStateType.EXECUTED) {
                continue;
            }
            if (context.tempRemoteObjectMap.get(operation.pKey).getValue() != null) {
                List<DataBox> dataBoxes = new ArrayList<>();
                StringDataBox stringDataBox = new StringDataBox();
                stringDataBox.setString(context.tempRemoteObjectMap.get(operation.pKey).getValue());
                dataBoxes.add(stringDataBox);
                dataBoxes.add(stringDataBox);
                SchemaRecord readRecord = new SchemaRecord(dataBoxes);
                operation.function.getStateObject(operation.stateObjectName.get(0)).setSchemaRecord(readRecord);
                //UDF updates operation.udfResult, which is the value to be written to writeRecord
                boolean udfSuccess = clientObj.transactionUDF(operation.function);
                AppConfig.randomDelay();//To quantify the overhead of user-defined function
                if (udfSuccess) {
                    operation.operationType = MetaTypes.OperationStateType.EXECUTED;
                } else {
                    operation.function.setAborted();
                    operation.operationType = MetaTypes.OperationStateType.ABORTED;
                    operation.notifyChildren();
                }
            }
        }
    }
    private void commit(Context context) {
        for (Operation operation : context.tempOperations) {
            if (operation.accessType == CommonMetaTypes.AccessType.WRITE) {
                try {
                    this.remoteStorageManager.asyncWriteRemoteDatabase(operation.table_name, operation.pKey, operation.function.udfResult,this.rdmaWorkerManager);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            operation.operationType = MetaTypes.OperationStateType.COMMITTED;
        }
    }
    private void releaseLock(Context context) {
        try {
            for (Operation operation : context.tempOperations) {
                if (operation.accessType == CommonMetaTypes.AccessType.WRITE && context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                    this.remoteStorageManager.exclusiveLockRelease(operation.table_name, operation.pKey,this.rdmaWorkerManager);
                    context.tempRemoteObjectMap.get(operation.pKey).setSuccessLocked(false);
                } else if (operation.accessType == CommonMetaTypes.AccessType.READ && context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                    this.remoteStorageManager.sharedLockRelease(operation.table_name, operation.pKey,this.rdmaWorkerManager);
                    context.tempRemoteObjectMap.get(operation.pKey).setSuccessLocked(false);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void AddContext(int thisTaskId, Context context) {
        threadToContext.put(thisTaskId, context);
    }
}