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
import intellistream.morphstream.engine.txn.scheduler.context.ds.OCCContext;
import intellistream.morphstream.engine.txn.scheduler.impl.RemoteStorageScheduler;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import intellistream.morphstream.util.AppConfig;
import lombok.Getter;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class OCCScheduler<Context extends OCCContext> extends RemoteStorageScheduler<Context> {
    private static final Logger LOG = Logger.getLogger(OCCScheduler.class);
    public final RdmaWorkerManager rdmaWorkerManager;
    public final RemoteStorageManager remoteStorageManager;
    @Getter
    private final ConcurrentHashMap<Integer, Context> threadToContext = new ConcurrentHashMap<>();//threadId -> context
    private final ConcurrentHashMap<String, OCCContext.RemoteObject> dataCache = new ConcurrentHashMap<>();//key -> remoteObject
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
    public OCCScheduler(RdmaWorkerManager rdmaWorkerManager, RemoteStorageManager remoteStorageManager) {
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
        while(!context.successfullyCommit()){
            //Read
            asyncRead(context);
            //Execution
            while (context.hasUnExecuted()) {
                execute(context);
            }
            //TryToCommit
            if (context.canCommit()) {
                if (getLock(context)) {
                    commit(context);
                } else {
                    releaseLock(context);
                }
            } else {
                return;
            }
            //ReleaseLock
            releaseLock(context);
        }
    }


    public void asyncRead(Context context) {
        for (Operation operation : context.tempOperations) {
            try {
                if (dataCache.contains(operation.pKey))
                    context.tempRemoteObjectMap.put(operation.pKey, dataCache.get(operation.pKey));
                else {
                    this.remoteStorageManager.asyncReadRemoteDatabaseWithVersion(operation.table_name, operation.pKey, this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
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
    private boolean getLock(Context context) {
        try {
            for (Operation operation : context.tempOperations) {
                if (operation.accessType == CommonMetaTypes.AccessType.WRITE && !context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                   while (!context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                       boolean isAbortLock = this.remoteStorageManager.validationWriteLockAcquisition(operation.bid, operation.table_name, operation.pKey,this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
                       if (isAbortLock) {
                           this.dataCache.remove(operation.pKey);
                           return false;
                       }
                   }
                } else if (operation.accessType == CommonMetaTypes.AccessType.READ && !context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                    boolean isAbortLock = this.remoteStorageManager.validationReadAcquisition(operation.table_name, operation.pKey,this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
                    if (isAbortLock) {
                        this.dataCache.remove(operation.pKey);
                        return false;
                    }
                }
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private void commit(Context context) {
        for (Operation operation : context.tempOperations) {
            if (operation.accessType == CommonMetaTypes.AccessType.WRITE) {
                try {
                    this.remoteStorageManager.asyncWriteRemoteDatabaseWithVersion(operation.table_name, operation.pKey, operation.function.udfResult, this.rdmaWorkerManager, context.tempRemoteObjectMap.get(operation.pKey));
                } catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
            operation.operationType = MetaTypes.OperationStateType.COMMITTED;
            if (this.dataCache.containsKey(operation.pKey)) {
                this.dataCache.get(operation.pKey).setSuccessLocked(false);
                this.dataCache.get(operation.pKey).setVersion(context.tempRemoteObjectMap.get(operation.pKey).getVersion());
                this.dataCache.get(operation.pKey).setValue(context.tempRemoteObjectMap.get(operation.pKey).getValue());
            } else {
                OCCContext.RemoteObject remoteObject = new OCCContext.RemoteObject();
                remoteObject.setSuccessLocked(false);
                remoteObject.setVersion(context.tempRemoteObjectMap.get(operation.pKey).getVersion());
                remoteObject.setValue(context.tempRemoteObjectMap.get(operation.pKey).getValue());
                this.dataCache.put(operation.pKey, remoteObject);
            }
        }
    }
    private void releaseLock(Context context) {
        try {
            for (Operation operation : context.tempOperations) {
                if (operation.accessType == CommonMetaTypes.AccessType.WRITE && context.tempRemoteObjectMap.get(operation.pKey).isSuccessLocked()) {
                    this.remoteStorageManager.exclusiveLockRelease(operation.table_name, operation.pKey,this.rdmaWorkerManager);
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
