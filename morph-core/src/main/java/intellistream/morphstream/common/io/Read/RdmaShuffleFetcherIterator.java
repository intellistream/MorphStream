package intellistream.morphstream.common.io.Read;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Exception.rdma.MetadataFetchFailedException;
import intellistream.morphstream.common.io.Rdma.*;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Block.BlockId;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Block.BlockManagerId;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Block.RdmaShuffleManagerId;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Stats.RdmaShuffleReaderStats;
import intellistream.morphstream.common.io.Read.Result.FetchResult;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RdmaShuffleFetcherIterator implements Iterator<InputStream> {
    private final Logger LOG = LoggerFactory.getLogger(RdmaShuffleFetcherIterator.class);
    private final int startPartition;
    private final int endPartition;
    private final int shuffleId;
    private final List<Pair<BlockManagerId, List<Pair<BlockId, Long>>>> blocksByAddress;
    // numBlocksToFetch is initialized with "1" so hasNext() will return ture until all the remote
    // fetches has been started. The remaining extra "1" will be fulfilled with a null InputStream in
    // insertDummyResult()
    private AtomicInteger numBlocksToFetch = new AtomicInteger();
    private int numBlocksProcessed = 0;
    private RdmaShuffleManager rdmaShuffleManager = MorphStreamEnv.get().RM();
    private LinkedBlockingQueue<FetchResult> resultsQueue = new LinkedBlockingQueue();
    private volatile FetchResult currentResult = null;
    private volatile boolean isStopped = false;
    private BlockManagerId localBlockManagerId = MorphStreamEnv.get().blockManagerId();
    private RdmaShuffleConf rdmaShuffleConf = rdmaShuffleManager.conf;
    private AtomicLong curBytesInFlight = new AtomicLong(0);
    private Random rand = new Random(System.nanoTime());
    //Make random ordering of pending fetches to prevent over-subscription to channel
    private PriorityBlockingQueue<PendingFetch> pendingFetchesQueue = new PriorityBlockingQueue<>(100, new Comparator<PendingFetch>() {
        @Override
        public int compare(PendingFetch o1, PendingFetch o2) {
            return -1 + rand.nextInt(3);
        }
    });
    public RdmaShuffleReaderStats rdmaShuffleReaderStats = rdmaShuffleManager.rdmaShuffleReaderStats;
    private final int rdmaReadRequestsLimit = rdmaShuffleConf.sendQueueDepth /
            Integer.parseInt(rdmaShuffleConf.getConfKey("morphstream.executor.cores"));
    public RdmaShuffleFetcherIterator(int startPartition,
                                      int endPartition,
                                      int shuffleId, List<Pair<BlockManagerId, List<Pair<BlockId, Long>>>> blocksByAddress) {
        this.startPartition = startPartition;
        this.endPartition = endPartition;
        this.shuffleId = shuffleId;
        this.blocksByAddress = blocksByAddress;
    }
    public void insertDummyResult() {
        if (!isStopped) {
            resultsQueue.add(new FetchResult.SuccessFetchResult(startPartition, localBlockManagerId, null));
        }
    }
    private void fetchBlocks(PendingFetch pendingFetch) throws InterruptedException {
        long startRemoteFetchTime = System.currentTimeMillis();
        RdmaRegisteredBuffer rdmaRegisteredBuffer = null;
        List<RdmaByteBufferManagedBuffer> rdmaByteBufferManagedBuffers = new ArrayList<>();
        try {
            //Allocate memory for incoming block fetches
            rdmaRegisteredBuffer = rdmaShuffleManager.getRdmaRegisteredBuffer(pendingFetch.getTotalLength());
            for (RdmaBlockLocation rdmaBlockLocation : pendingFetch.rdmaBlockLocations) {
                RdmaByteBufferManagedBuffer buffer = new RdmaByteBufferManagedBuffer(rdmaRegisteredBuffer, rdmaBlockLocation.length);
                rdmaByteBufferManagedBuffers.add(buffer);
            }
        } catch (IOException e) {
            if (rdmaRegisteredBuffer != null) rdmaRegisteredBuffer.release();
            LOG.error("Failed to allocate memory for incoming block fetches, failing pending" + " block fetches. " + e);
            resultsQueue.put(new FetchResult.FailureFetchResult(startPartition, null, e));
            throw new RuntimeException(e);
        }
        //Send RDMA read requests
        //0. Define rdmaCompletionListener
        RdmaCompletionListener rdmaCompletionListener = new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buffer) {
                for (RdmaByteBufferManagedBuffer buf : rdmaByteBufferManagedBuffers) {
                   if (!isStopped) {
                       try {
                           //2. Put FetchResult in the allocated buffer
                           InputStream inputStream = new BufferReleasingInputStream(buf.createInputStream(), buf);
                           resultsQueue.put(new FetchResult.SuccessFetchResult(startPartition, pendingFetch.rdmaShuffleManagerId.getBlockManagerId(), inputStream));
                       } catch (IOException | InterruptedException e) {
                           throw new RuntimeException(e);
                       }
                   } else {
                       buf.release();
                   }
                }
                if (rdmaShuffleReaderStats != null) {
                    rdmaShuffleReaderStats.updateRemoteFetchHistogram(pendingFetch.rdmaShuffleManagerId.getBlockManagerId(), (int) (System.currentTimeMillis() - startRemoteFetchTime));
                }
                LOG.info("Got remote block(s): of size" + pendingFetch.totalLength  + " from " + pendingFetch.rdmaShuffleManagerId.getBlockManagerId()  + " after " + (System.currentTimeMillis() - startRemoteFetchTime) + " ms");
            }
            @Override
            public void onFailure(Throwable cause) {
                LOG.error("Failed to fetch remote block(s) of size " + pendingFetch.totalLength + " from " + pendingFetch.rdmaShuffleManagerId.getBlockManagerId() + " after " + (System.currentTimeMillis() - startRemoteFetchTime) + " ms", cause);
                resultsQueue.add(new FetchResult.FailureFetchResult(startPartition, pendingFetch.rdmaShuffleManagerId.getBlockManagerId(), cause));
                for (RdmaByteBufferManagedBuffer buf : rdmaByteBufferManagedBuffers) {
                    buf.release();
                }
            }
        };
        //1. Send RDMA read requests
        try {
            RdmaChannel rdmaChannel = rdmaShuffleManager.getRdmaChannelOfREAD_REQUESTOR(pendingFetch.rdmaShuffleManagerId, true);
            int[] sizes = new int[pendingFetch.rdmaBlockLocations.size()];
            long[] addresses = new long[pendingFetch.rdmaBlockLocations.size()];
            int[] rKeys = new int[pendingFetch.rdmaBlockLocations.size()];
            for (int i = 0; i < pendingFetch.rdmaBlockLocations.size(); i++) {
                RdmaBlockLocation rdmaBlockLocation = pendingFetch.rdmaBlockLocations.get(i);
                sizes[i] = rdmaBlockLocation.length;
                addresses[i] = rdmaBlockLocation.address;
                rKeys[i] = rdmaBlockLocation.mKey;
            }
            rdmaChannel.rdmaReadInQueue(rdmaCompletionListener, rdmaRegisteredBuffer.getRegisteredAddress(),rdmaRegisteredBuffer.getLkey(), sizes, addresses, rKeys);
        } catch (IOException e) {
            rdmaCompletionListener.onFailure(e);
            throw new RuntimeException(e);
        }
    }
    private void startAsyncRemoteFetches() throws InterruptedException {
        //0. Get the whole MapTaskOutputAddressTable
        RdmaBuffer rdmaBuffer = null;
        ByteBuffer mapTaskOutput = null;
        List<Pair<BlockManagerId, List<Pair<BlockId, Long>>>> groupedBlocksByAddress = new ArrayList<>();
        AtomicInteger totalRemainingLocations = new AtomicInteger(0);
        try {
            rdmaBuffer = rdmaShuffleManager.getMapTaskOutputTable(shuffleId).get();
            mapTaskOutput = rdmaBuffer.getByteBuffer();
            for (Pair<BlockManagerId, List<Pair<BlockId, Long>>> pair : blocksByAddress) {
                if (!pair.getKey().equals(localBlockManagerId)) {
                    List<Pair<BlockId, Long>> filteredList = new ArrayList<>();
                    for (Pair<BlockId, Long> blockPair : pair.getValue()) {
                        if (blockPair.getValue() > 0) {
                            filteredList.add(blockPair);
                        }
                    }
                    if (!filteredList.isEmpty()) {
                        totalRemainingLocations.addAndGet(filteredList.size());
                        groupedBlocksByAddress.add(new Pair<>(pair.getKey(), filteredList));
                    }
                }
            }
        } catch (ExecutionException | IOException e) {
            resultsQueue.put(new FetchResult.FailureMetadataFetchResult(new MetadataFetchFailedException(shuffleId, startPartition, e.getMessage())));
            LOG.error("Failed to RDMA read MapTaskOutputAddressTable: " + e);
            return;
        }
        if (totalRemainingLocations.get() == 0) {
            insertDummyResult();
        }
        for (Pair<BlockManagerId, List<Pair<BlockId, Long>>> pair : groupedBlocksByAddress) {
           RdmaShuffleManagerId requestedRdmaShuffleManagerId = null;
           try{
               requestedRdmaShuffleManagerId = rdmaShuffleManager.blockManagerIdToRdmaShuffleManagerId.get(pair.getKey());
              } catch (NoSuchElementException e) {
               LOG.error("RdmaShuffleNode: " + localBlockManagerId + " has no RDMA connection to " + pair.getKey());
               resultsQueue.put(new FetchResult.FailureMetadataFetchResult(new MetadataFetchFailedException(shuffleId, startPartition, e.getMessage())));
               return;
           }
           List<List<Pair<BlockId, Long>>> groups = new ArrayList<>();
           List<Pair<BlockId, Long>> currentGroup = new ArrayList<>();
           for (int i = 0; i < pair.getValue().size(); i ++) {
               currentGroup.add(pair.getValue().get(i));
               if (currentGroup.size() == rdmaReadRequestsLimit || i == pair.getValue().size() - 1) {
                   groups.add(new ArrayList<>(currentGroup));
                   currentGroup.clear();
               }
           }
           for (List<Pair<BlockId, Long>> group : groups) {

               for (Pair<BlockId, Long> blockPair : group) {

               }
           }
        }
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public InputStream next() {
        return null;
    }


    public class PendingFetch {
        private RdmaShuffleManagerId rdmaShuffleManagerId;
        private List<RdmaBlockLocation> rdmaBlockLocations;
        private int totalLength;

        public PendingFetch(
                RdmaShuffleManagerId rdmaShuffleManagerId,
                List<RdmaBlockLocation> rdmaBlockLocations,
                int totalLength) {
            this.rdmaShuffleManagerId = rdmaShuffleManagerId;
            this.rdmaBlockLocations = rdmaBlockLocations;
            this.totalLength = totalLength;
        }

        public RdmaShuffleManagerId getRdmaShuffleManagerId() {
            return rdmaShuffleManagerId;
        }

        public List<RdmaBlockLocation> getRdmaBlockLocations() {
            return rdmaBlockLocations;
        }

        public int getTotalLength() {
            return totalLength;
        }
    }
    private class BufferReleasingInputStream extends InputStream{
        private InputStream delegate;
        private RdmaByteBufferManagedBuffer buffer;
        private boolean closed = false;
        public BufferReleasingInputStream(InputStream delegate, RdmaByteBufferManagedBuffer buffer) {
            this.delegate = delegate;
            this.buffer = buffer;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }
        @Override
        public void close() throws IOException {
            if (!closed) {
                delegate.close();
                buffer.release();
                closed = true;
            }
        }
    }


}
