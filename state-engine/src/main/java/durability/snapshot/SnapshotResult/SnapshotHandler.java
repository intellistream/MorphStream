package durability.snapshot.SnapshotResult;


import utils.FaultToleranceConstants;

import java.io.IOException;
import java.nio.channels.CompletionHandler;

public class SnapshotHandler implements CompletionHandler<Integer, Attachment> {
    @Override
    public void completed(Integer result, Attachment attach) {
        try {
            SnapshotResult snapshotResult = attach.getSnapshotResult();
            snapshotResult.size = result / 1024;
            attach.ftManager.boltRegister(attach.partitionId,
                    FaultToleranceConstants.FaultToleranceStatus.Snapshot,
                    snapshotResult);
            attach.asyncChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void failed(Throwable exc, Attachment attach) {
        try {
            //TODO: notify the operator to reExecute snapshot
            attach.asyncChannel.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }
}
