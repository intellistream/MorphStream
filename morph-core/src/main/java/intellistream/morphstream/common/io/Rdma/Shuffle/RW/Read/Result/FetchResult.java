package intellistream.morphstream.common.io.Rdma.Shuffle.RW.Read.Result;

import intellistream.morphstream.common.io.Exception.rdma.MetadataFetchFailedException;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Block.BlockManagerId;

import java.io.InputStream;

public interface FetchResult {
    class SuccessFetchResult implements FetchResult {
        private int partitionId;
        private BlockManagerId blockManagerId;
        private InputStream inputStream;

        public SuccessFetchResult(int partitionId, BlockManagerId blockManagerId, InputStream inputStream) {
            this.partitionId = partitionId;
            this.blockManagerId = blockManagerId;
            this.inputStream = inputStream;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public BlockManagerId getBlockManagerId() {
            return blockManagerId;
        }

        public InputStream getInputStream() {
            return inputStream;
        }
    }
    class FailureFetchResult implements FetchResult {
        private int partitionId;
        private BlockManagerId blockManagerId;
        private Throwable e;

        public FailureFetchResult(int partitionId, BlockManagerId blockManagerId, Throwable e) {
            this.partitionId = partitionId;
            this.blockManagerId = blockManagerId;
            this.e = e;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public BlockManagerId getBlockManagerId() {
            return blockManagerId;
        }

        public Throwable getException() {
            return e;
        }
    }
    class FailureMetadataFetchResult implements FetchResult {
        private MetadataFetchFailedException e;

        public FailureMetadataFetchResult(MetadataFetchFailedException e) {
            this.e = e;
        }

        public MetadataFetchFailedException getException() {
            return e;
        }
    }

}
