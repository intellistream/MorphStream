package durability.snapshot.SnapshotStrategy.ImplSnapshotStrategy;

import common.io.ByteIO.DataInputView;
import common.io.ByteIO.InputWithDecompression.NativeDataInputView;
import common.io.ByteIO.InputWithDecompression.SnappyDataInputView;
import common.tools.Deserialize;
import durability.ftmanager.AbstractRecoveryManager;
import durability.ftmanager.FTManager;
import durability.snapshot.SnapshotOptions;
import durability.snapshot.SnapshotResources.ImplSnapshotResources.InMemoryFullSnapshotResources;
import durability.snapshot.SnapshotResources.StateMetaInfoSnapshot;
import durability.snapshot.SnapshotResult.Attachment;
import durability.snapshot.SnapshotResult.SnapshotHandler;
import durability.snapshot.SnapshotResult.SnapshotResult;
import durability.snapshot.SnapshotStrategy.SnapshotStrategy;
import durability.snapshot.SnapshotStream.ImplSnapshotStreamFactory.NIOSnapshotStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.table.BaseTable;
import storage.table.RecordSchema;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;
import static utils.FaultToleranceConstants.CompressionType.None;

public class InMemorySnapshotStrategy implements SnapshotStrategy<InMemoryFullSnapshotResources> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemorySnapshotStrategy.class);
    @Nonnull protected Map<String, BaseTable> tables;
    @Nonnull protected SnapshotOptions snapshotOptions;
    @Nonnull protected String snapshotPath;
    @Nonnull protected ConcurrentHashMap<String, InMemoryKvStateInfo> kvStateInformation = new ConcurrentHashMap<>();
    private static final String DESCRIPTION = "Full snapshot of In-Memory Database";

    public InMemorySnapshotStrategy(Map<String, BaseTable> tables, SnapshotOptions snapshotOptions, String snapshotPath) {
        this.tables = tables;
        this.snapshotOptions = snapshotOptions;
        this.snapshotPath = snapshotPath;
    }
    @Override
    public InMemoryFullSnapshotResources syncPrepareResources(long snapshotId, int partitionId) {
        return new InMemoryFullSnapshotResources(snapshotId, partitionId, kvStateInformation, tables);
    }

    @Override
    public void registerTable(String tableName, RecordSchema r) {
        InMemoryKvStateInfo inMemoryKvStateInfo = new InMemoryKvStateInfo(tableName, r);
        this.kvStateInformation.put(tableName, inMemoryKvStateInfo);
    }

    @Override
    public void asyncSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws IOException {
        NIOSnapshotStreamFactory nioSnapshotStreamFactory = new NIOSnapshotStreamFactory(this.snapshotPath, snapshotOptions.getCompressionAlg());
        InMemoryFullSnapshotResources inMemoryFullSnapshotResources = syncPrepareResources(snapshotId, partitionId);
        AsynchronousFileChannel afc = nioSnapshotStreamFactory.createSnapshotStream();
        Attachment attachment = new Attachment(nioSnapshotStreamFactory.getSnapshotPath(), snapshotId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = inMemoryFullSnapshotResources.createWriteBuffer(snapshotOptions);
        afc.write(dataBuffer, 0, attachment, new SnapshotHandler());
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    public static class InMemoryKvStateInfo implements Serializable {
        public final String tableName;
        public final RecordSchema recordSchema;
        public InMemoryKvStateInfo(String tableName, RecordSchema recordSchema) {
            this.tableName = tableName;
            this.recordSchema = recordSchema;
        }
    }
    public void syncRecoveryFromSnapshot(SnapshotResult snapshotResult) throws IOException, ExecutionException, InterruptedException {
        String path = snapshotResult.path;
        Path snapshotPath = Paths.get(path);
        AsynchronousFileChannel afc = AsynchronousFileChannel.open(snapshotPath, READ);
        int fileSize = (int) afc.size();
        ByteBuffer dataBuffer = ByteBuffer.allocate(fileSize);
        Future<Integer> result = afc.read(dataBuffer, 0);
        int readBytes = result.get();
        DataInputView inputView;
        if (snapshotOptions.getCompressionAlg() != None){
            inputView = new SnappyDataInputView(dataBuffer);//Default to use Snappy compression
        } else {
            inputView = new NativeDataInputView(dataBuffer);
        }
        int stateMetaInfoSize = inputView.readInt();
        StateMetaInfoSnapshot[] stateMetaInfoSnapshots = new StateMetaInfoSnapshot[stateMetaInfoSize];
        for (int i = 0; i < stateMetaInfoSize; i++) {
            byte[] objects = inputView.readFullyDecompression();
            stateMetaInfoSnapshots[i] = (StateMetaInfoSnapshot) Deserialize.Deserialize(objects);
        }
        for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
            int recordNum = stateMetaInfoSnapshot.recordNum;
            while(recordNum != 0) {
                byte[] objects = inputView.readFullyDecompression();
                SchemaRecord schemaRecord;
                schemaRecord = AbstractRecoveryManager.getRecord(stateMetaInfoSnapshot.recordSchema, objects);
                this.tables.get(stateMetaInfoSnapshot.tableName).SelectKeyRecord(schemaRecord.GetPrimaryKey()).content_.updateMultiValues(snapshotResult.snapshotId, 0L, false, schemaRecord);
                recordNum --;
            }
        }
        LOG.info("Reload Database complete: " + snapshotResult.partitionId);
    }
}