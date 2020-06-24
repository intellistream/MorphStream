/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */

package application.bolts.lr;

import application.datatype.util.TimeStampExtractor;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.NoSuchElementException;


/**
 * {@link StreamMerger} merges multiple sub-stream in ascending timestamp order. Type {@code T} is expected to be either
 * {@link Tuple}
 *
 * @author Matthias J. Sax
 */
// TODO: make more efficient (avoid linear scan of all partitions to extract next tuple)
public class StreamMerger<T> {
    private final static Logger logger = LoggerFactory.getLogger(StreamMerger.class);

    /**
     * The index of the timestamp attribute ({@code -1} if attribute name or timestamp extractor is used).
     */
    private final int tsIndex;

    /**
     * The name of the timestamp attribute ({@code null} if attribute index or timestamp extractor is used).
     */
    private final String tsAttributeName;

    /**
     * The extractor for the timestamp ({@code null} if attribute index or name is used).
     */
    private final TimeStampExtractor<T> tsExtractor;

    /**
     * Input tuple buffer for merging. Contains a list of input tuples for each producer Task.
     */
    private final HashMap<Integer, LinkedList<T>> mergeBuffer = new HashMap<Integer, LinkedList<T>>();

    /**
     * Maximum timestamp value_list that was emitted already;
     */
    private long latestTs = Long.MIN_VALUE;


    /**
     * Instantiates a new {@link StreamMerger}. Must be used if {@code T} is type {@link Tuple}.
     *
     * @param partitionIds The IDs of the sub-streams (partitions) that should be merged.
     * @param tsIndex      The index of the timestamp attribute.
     */
    public StreamMerger(Collection<Integer> partitionIds, int tsIndex) {
        assert (partitionIds != null);
        assert (tsIndex >= 0);

        logger.debug("Initializing with timestamp index: {}", tsIndex);

        this.tsIndex = tsIndex;
        this.tsAttributeName = null;
        this.tsExtractor = null;

        this.initialize(partitionIds);
    }

    /**
     * Instantiates a new {@link StreamMerger}. Can only be used if {@code T} is type {@link Tuple}.
     *
     * @param tsAttributeName The name of the timestamp attribute.
     */
    public StreamMerger(Collection<Integer> partitionIds, String tsAttributeName) {
        assert (partitionIds != null);
        assert (tsAttributeName != null);

        logger.debug("Initializing with timestamp attribute: {}", tsAttributeName);

        this.tsIndex = -1;
        this.tsAttributeName = tsAttributeName;
        this.tsExtractor = null;

        this.initialize(partitionIds);
    }

    /**
     * Instantiates a new {@link StreamMerger}. Can only be used if {@code T} is type {@link Tuple}.
     *
     * @param tsExtractor The extractor for the timestamp.
     */
    public StreamMerger(Collection<Integer> partitionIds, TimeStampExtractor<T> tsExtractor) {
        assert (partitionIds != null);
        assert (tsExtractor != null);

        logger.debug("Initializing with timestamp extractor:");

        this.tsIndex = -1;
        this.tsAttributeName = null;
        this.tsExtractor = tsExtractor;

        this.initialize(partitionIds);
    }

    private void initialize(Collection<Integer> partitionIds) {
        logger.debug("Initializing partition buffer: {}", partitionIds);
        for (Integer partition : partitionIds) {
            this.mergeBuffer.put(partition, new LinkedList<T>());
        }
    }


    /**
     * Adds a tuple belonging to partition {@code partitionNumber} to the internal merging buffer. Assumes, that the
     * timestamp of the inserted tuple is not smaller
     */
    public void addTuple(Integer partitionNumber, T t) {
        logger.trace("Add tuple to buffer (partitionId, tuple): {}, {}", partitionNumber, this.getTsValue(t));
        assert (partitionNumber != null);
        assert (t != null);
        LinkedList<T> partitionBuffer = this.mergeBuffer.get(partitionNumber);
        assert (partitionBuffer != null);

        assert (partitionBuffer.size() == 0 || this.getTsValue(partitionBuffer.getLast()) <= this.getTsValue(t));

        partitionBuffer.addLast(t);
    }

    /**
     * Returns the next tuple from the internal merging buffer. A tuple can be returned, if it has the same timestamp as
     * the last extracted tuple. If all tuples have a larger timestamp than the last returned tuple, the tuple with the
     * smallest timestamp is returned iff at least one tuple is present in each buffer.
     *
     * @return The next tuple in ascending timestamp order -- {@code null} if no tuple could be extracted.
     */
    public T getNextTuple() {
        long minTsFound = Long.MAX_VALUE;
        boolean eachBufferFilled = true;
        Integer minTsPartitionNumber = null;

        for (Entry<Integer, LinkedList<T>> partition : this.mergeBuffer.entrySet()) {
            LinkedList<T> partitionBuffer = partition.getValue();
            try {
                long ts = this.getTsValue(partitionBuffer.getFirst());


                if (ts < this.latestTs) {
                    System.nanoTime();
                }


                if (ts == this.latestTs) {
                    logger.trace("Extract tuple with same timestamp (partition, tuple): {}, {}", partition.getKey(),
                            partitionBuffer.getFirst());
                    return partitionBuffer.removeFirst();
                }

                if (ts < minTsFound) {
                    minTsFound = ts;
                    minTsPartitionNumber = partition.getKey();
                }
            } catch (NoSuchElementException e) {
                // in this case, we stay in the loop, because we still might find a tuple with equal ts value_list as last
                // returned tuple
                logger.trace("Found empty parition: {}", partition.getKey());
                eachBufferFilled = false;
            }
        }

        if (eachBufferFilled && minTsPartitionNumber != null) {
            logger.trace("Extract tuple min timestamp (ts, partition, tuple): {}, {}", new Long(minTsFound),
                    minTsPartitionNumber, this.mergeBuffer.get(minTsPartitionNumber).getFirst());
            this.latestTs = minTsFound;
            return this.mergeBuffer.get(minTsPartitionNumber).removeFirst();
        }

        logger.trace("Could not extract tuple.");
        return null;
    }

    private long getTsValue(T tuple) {
        if (this.tsIndex != -1) {
//            if (tuple instanceof JumboTuple) {
//                return ((Number) ((Tuple) tuple).getValue(this.tsIndex)).longValue();
//            }
//            assert (tuple instanceof JumboTuple);
            return ((Tuple) tuple).getBID(); //((PositionReport) (((Tuple) tuple).getValue(0))).getTime();

        }
//		if (this.tsAttributeName != null) {
//			return ((Number) ((Tuple) tuple).getValueByField(this.tsAttributeName)).longValue();
//		}

        return this.tsExtractor.getTs(tuple);
    }

    /**
     * Removes an empty partition from the internal buffer.
     * <p/>
     * Can be used to 'unblock' {@link StreamMerger} in case of a completely consumed partition. A empty partition
     * prevents {@link #getNextTuple()} to return tuples from the remaining (non-empty) partition buffers, because it
     * assumes that new data is inserted into the currently empty partition buffer later on. Hence, if it is guaranteed,
     * that a partition does not yield any more data, it must be removed for further processing of the remaining
     * partitions.<br/>
     * <br/>
     * <strong>Only empty partitions can be removed.</strong>
     *
     * @param partitionId The partition to be removed.
     * @return {@code true} if the partition was successfully removed -- {@code false} otherwise
     */
    public boolean closePartition(Integer partitionId) {
        if (this.mergeBuffer.get(partitionId).size() == 0) {
            logger.debug("Closing partition: {}", partitionId);
            this.mergeBuffer.remove(partitionId);
            return true;
        }

        logger.debug("Closing partition {} failed.", partitionId);
        return false;
    }
}
