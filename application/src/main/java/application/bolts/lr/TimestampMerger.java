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
import sesame.components.TopologyComponent;
import sesame.components.context.TopologyContext;
import sesame.components.grouping.Grouping;
import sesame.components.operators.api.AbstractBolt;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionNode;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import state_engine.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;


/**
 * {@link TimestampMerger} merges all incoming streams (all physical substreams from all tasks) over all logical
 * producers in ascending timestamp order. Input tuples must be in ascending timestamp order within each incoming
 * substream. The timestamp attribute is expected to be of type {@link Number}.
 * <p>
 * The internal buffer can be flushed by sending an <strong>ID less</strong> zero-attribute tuple via stream
 * {@link #FLUSH_STREAM_ID}.
 *
 * @author mjsax
 */

public class TimestampMerger extends MapBolt {
    private final static long serialVersionUID = -6930627449574381467L;
    private final static Logger LOG = LoggerFactory.getLogger(TimestampMerger.class);


    /**
     * The name of the flush stream.
     */
    public final static String FLUSH_STREAM_ID = "flush";

    /**
     * The original bolt that consumers a stream of input tuples that are ordered by their timestamp attribute.
     */
    private final AbstractBolt wrappedBolt;

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
    private final TimeStampExtractor<Tuple> tsExtractor;

    /**
     * Input tuple buffer for merging.
     */
    private StreamMerger<Tuple> merger;

    /**
     * Instantiates a new {@link TimestampMerger} that wrapped the given bolt.
     *
     * @param wrappedBolt The bolt to be wrapped.
     * @param tsIndex     The index of the timestamp attribute.
     */
    public TimestampMerger(AbstractBolt wrappedBolt, int tsIndex) {
        super(LOG, new HashMap<>());
        assert (wrappedBolt != null);
        assert (tsIndex >= 0);

        //LOG.DEBUG("Initialize with timestamp index {}", new Integer(tsIndex));

        this.wrappedBolt = wrappedBolt;
        this.tsIndex = tsIndex;
        this.tsAttributeName = null;
        this.tsExtractor = null;
    }

    /**
     * Instantiates a new {@link TimestampMerger} that wrapped the given bolt.
     *
     * @param wrappedBolt     The bolt to be wrapped.
     * @param tsAttributeName The name of the timestamp attribute.
     */
    public TimestampMerger(AbstractBolt wrappedBolt, String tsAttributeName) {
        super(LOG, new HashMap<>());
        assert (wrappedBolt != null);
        assert (tsAttributeName != null);

        LOG.debug("Initialize with timestamp attribute {}", tsAttributeName);

        this.wrappedBolt = wrappedBolt;
        this.tsIndex = -1;
        this.tsAttributeName = tsAttributeName;
        this.tsExtractor = null;
    }

    /**
     * Instantiates a new {@link TimestampMerger} that wrapped the given bolt.
     *
     * @param wrappedBolt The bolt to be wrapped.
     * @param tsExtractor The extractor for the timestamp.
     */
    public TimestampMerger(AbstractBolt wrappedBolt, TimeStampExtractor<Tuple> tsExtractor) {
        super(LOG, new HashMap<>());
        assert (wrappedBolt != null);
        assert (tsExtractor != null);

        //LOG.DEBUG("Initialize with timestamp extractor");

        this.wrappedBolt = wrappedBolt;
        this.tsIndex = -1;
        this.tsAttributeName = null;
        this.tsExtractor = tsExtractor;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        // for each logical input stream (ie, each producer bolt), we GetAndUpdate an input partition for each of its tasks
        LinkedList<Integer> taskIds = new LinkedList<Integer>();
        HashMap<String, Map<TopologyComponent, Grouping>> parents = context.getThisComponent().getParents();

        for (Map<TopologyComponent, Grouping> map : parents.values()) {
            for (TopologyComponent topologyComponent : map.keySet()) {
                for (ExecutionNode producer : topologyComponent.getExecutorList()) {
                    taskIds.add(producer.getExecutorID());
                }
            }

        }

        //LOG.DEBUG("Detected producer tasks: {}", taskIds);

        if (this.tsIndex != -1) {
            assert (this.tsAttributeName == null && this.tsExtractor == null);
            this.merger = new StreamMerger<>(taskIds, this.tsIndex);
        } else if (this.tsAttributeName != null) {
            assert (this.tsExtractor == null);
            this.merger = new StreamMerger<>(taskIds, this.tsAttributeName);
        } else {
            assert (this.tsExtractor != null);
            this.merger = new StreamMerger<>(taskIds, this.tsExtractor);
        }

        this.wrappedBolt.prepare(conf, context, outputCollector);
    }

    @Override
    public void execute(Tuple tuple) throws InterruptedException, DatabaseException, BrokenBarrierException {

        LOG.trace("Adding tuple to internal buffer tuple: {}", tuple.getBID());
        this.merger.addTuple(tuple.getSourceTask(), tuple);

        Tuple t;
        while ((t = this.merger.getNextTuple()) != null) {
            LOG.trace("Extrated tuple from internal buffer for processing: {}", tuple.getBID());
            this.wrappedBolt.execute(t);
        }
    }


    @Override
    public void cleanup() {
        this.wrappedBolt.cleanup();
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.wrappedBolt.declareOutputFields(declarer);
    }
}
