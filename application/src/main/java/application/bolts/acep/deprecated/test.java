package application.bolts.acep.deprecated;

/*
 * Copyright 2015 Variacode
 *
 * Licensed under the GPLv2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.espertech.esper.client.soda.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class test {

    private static final String LITERAL_SYMBOL = "symbol";
    private static final String LITERAL_PRICE = "price";
    private static final String LITERAL_RETURN_OBJ = "Result";
    private static final String LITERAL_AVG = "avg";
    private static final String LITERAL_ESPER = "esper";
    private static final String LITERAL_QUOTES = "quotes";
    private static Map<Integer, Double> resultEPL = new HashMap<>();
    private static Map<Integer, Double> resultSODA = new HashMap<>();

    /**
     * Test of execute method, of class EsperBolt.
     */
    @Test
    public void testSODA() {
        resultSODA = new HashMap<>();

        Logger.getLogger(test.class.getName()).log(Level.INFO, "EngineTest-SODA");

        //should say fieldsTypes, maybe with object/component prefix
        Map<String, Object> eventTypes = new HashMap<>();
        eventTypes.put(LITERAL_SYMBOL, String.class);
        eventTypes.put(LITERAL_PRICE, Integer.class);

        EPStatementObjectModel model = new EPStatementObjectModel();
        model.setInsertInto(InsertIntoClause.create(LITERAL_RETURN_OBJ));
        model.setSelectClause(SelectClause.create()
                .add(Expressions.avg(LITERAL_PRICE), LITERAL_AVG)
                .add(LITERAL_PRICE)
        );
        Filter filter = Filter.create("quotes_default", Expressions.eq(LITERAL_SYMBOL, "A"));
        model.setFromClause(FromClause.create(
                FilterStream.create(filter)
                        .addView("win", "length", Expressions.constant(2))
        ));
        model.setHavingClause(Expressions.gt(Expressions.avg(LITERAL_PRICE), Expressions.constant(60.0)));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(LITERAL_QUOTES, new RandomSentenceSpout());
//        builder.setBolt(LITERAL_ESPER, (new EsperBolt())
//                .addEventTypes(eventTypes)
//                .addOutputTypes(Collections.singletonMap(LITERAL_RETURN_OBJ, Arrays.asList(LITERAL_AVG, LITERAL_PRICE)))
//                .addObjectStatemens(Collections.singleton(model)))
//                .shuffleGrouping(LITERAL_QUOTES);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping(LITERAL_ESPER, LITERAL_RETURN_OBJ);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
        assertEquals(resultSODA.get(100), new Double(75.0));
        assertEquals(resultSODA.get(50), new Double(75.0));
    }

    /**
     * Test of execute method, of class EsperBolt.
     */
    @Test
    public void testEPL() {
        resultEPL = new HashMap<>();

        Logger.getLogger(test.class.getName()).log(Level.INFO, "EngineTest-EPL");

        //should say fieldsTypes, maybe with object/component prefix
        Map<String, Object> eventTypes = new HashMap<>();
        eventTypes.put(LITERAL_SYMBOL, String.class);
        eventTypes.put(LITERAL_PRICE, Integer.class);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(LITERAL_QUOTES, new RandomSentenceSpout());
//        builder.setBolt(LITERAL_ESPER, (new EsperBolt(-1))
//                .addEventTypes(eventTypes)
//                .addOutputTypes(Collections.singletonMap(LITERAL_RETURN_OBJ, Arrays.asList(LITERAL_AVG, LITERAL_PRICE)))
//                .addStatements(Collections.singleton("put into Result "
//                        + "select avg(price) as avg, price from "
//                        + "quotes_default(symbol='A').win:length(2) "
//                        + "having avg(price) > 60.0")))
//                .shuffleGrouping(LITERAL_QUOTES);

        builder.setBolt("print", new PrinterBolt()).shuffleGrouping(LITERAL_ESPER, LITERAL_RETURN_OBJ);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
        assertEquals(resultEPL.get(100), new Double(75.0));
        assertEquals(resultEPL.get(50), new Double(75.0));
    }

    /**
     *
     */
    public static class PrinterBolt extends BaseBasicBolt {

        /**
         * @param tuple
         * @param collector
         */
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            resultEPL.put(tuple.getIntegerByField(LITERAL_PRICE), tuple.getDoubleByField(LITERAL_AVG));
            resultSODA.put(tuple.getIntegerByField(LITERAL_PRICE), tuple.getDoubleByField(LITERAL_AVG));
        }

        /**
         * @param ofd
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            //Not implemented
        }

    }

    /**
     *
     */
    public static class RandomSentenceSpout extends BaseRichSpout {

        transient Queue<HashMap.SimpleEntry<String, Integer>> data;
        transient SpoutOutputCollector collector;
        transient int i;

        /**
         * @param conf
         * @param context
         * @param collector
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            data = new ConcurrentLinkedQueue<>();
            data.add(new HashMap.SimpleEntry<>("A", 50));
            data.add(new HashMap.SimpleEntry<>("A", 100));
            data.add(new HashMap.SimpleEntry<>("A", 50));
            data.add(new HashMap.SimpleEntry<>("B", 50));
            data.add(new HashMap.SimpleEntry<>("A", 30));
            data.add(new HashMap.SimpleEntry<>("C", 50));
            data.add(new HashMap.SimpleEntry<>("A", 50));
        }

        /**
         *
         */
        @Override
        public void nextTuple() {
            Utils.sleep(500);
            HashMap.SimpleEntry<String, Integer> d = this.data.poll();
            if (d != null) {
                this.collector.emit(new Values(d.getKey(), d.getValue()));
            }

        }

        /**
         * @param id
         */
        @Override
        public void ack(Object id) {
            //Not implemented
        }

        /**
         * @param id
         */
        @Override
        public void fail(Object id) {
            //Not implemented
        }

        /**
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(LITERAL_SYMBOL, LITERAL_PRICE));
        }

    }

}