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

import application.datatype.AbstractLRBTuple;
import application.datatype.DailyExpenditureRequest;
import application.datatype.toll.MemoryTollDataStore;
import application.datatype.toll.TollDataStore;
import application.datatype.util.Constants;
import application.datatype.util.LRTopologyControl;
import application.util.Configuration;
import application.util.OsUtils;
import application.util.events.HistoryEvent;
import application.util.lr.Helper;
import sesame.components.context.TopologyContext;
import sesame.components.operators.base.filterBolt;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


/**
 * Stub for daily expenditure queries. Responds to {@link DailyExpenditureRequest}s with tuple in the form of (Type = 3,
 * Time (specifying the time that d was emitted), Emit (specifying the time the query response is emitted), QID
 * (identifying the query that issued the request), Bal (the sum of all tolls from expressway x on day n that were
 * charged to the vehi- cle’s account). Reads from {@link LRTopologyControl#DAILY_EXPEDITURE_REQUESTS_STREAM_ID} and emits
 * tuple on {@link }.
 *
 * @TODO either use external distributed database to keep historic data or Prepared it into memory
 */
public class DailyExpenditureBolt extends filterBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DailyExpenditureBolt.class);
    private LinkedList<HistoryEvent> historyEvtList;
    private transient TollDataStore dataStore;

    /**
     *
     */
    public DailyExpenditureBolt() {
        super(LOG, new HashMap<>(), new HashMap<>());
        this.input_selectivity.put(LRTopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID, 1.0);
        this.setStateful();
    }

    public TollDataStore getDataStore() {
        return this.dataStore;
    }

    /**
     * initializes the used {@link TollDataStore} using the string specified as value_list to the
     * {@link Helper#TOLL_DATA_STORE_CONF_KEY} map key.
     *
     * @param conf
     * @param context
     * @param collector
     */
    /*
     * internal implementation notes: - due to the fact that compatibility is incapable of serializing Class property, a String
     * has to be passed in conf
     */
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        @SuppressWarnings("unchecked")

        String tollDataStoreClass = MemoryTollDataStore.class.getName();//(String) conf.GetAndUpdate(Helper.TOLL_DATA_STORE_CONF_KEY);
        try {
            this.dataStore = (TollDataStore) Class.forName(tollDataStoreClass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
            throw new RuntimeException(String.format("The data Store instance '%s' could not be initialized (see "
                    + "nested exception for details)", this.dataStore), ex);
        }


        //historyEvtList = new LinkedList<HistoryEvent>();
        String OS_prefix = null;
        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }

        String historyFile;
        if (OsUtils.isMac()) {
            historyFile = System.getProperty("user.home").concat("/Documents/data/app/").concat((String) conf.get(OS_prefix.concat("test.linear-history-file")));
        } else {
            historyFile = System.getProperty("user.home").concat("/Documents/data/app/").concat((String) conf.get(OS_prefix.concat("linear-history-file")));
        }


        loadHistoricalInfo(historyFile, conf, OS_prefix);
        Configuration config = Configuration.fromMap(conf);

    }

    private void loadHistoricalInfo(String inputFileHistory, Map conf, String OS_prefix) {
        BufferedReader in;
        try {
            try {
                in = new BufferedReader(new FileReader(inputFileHistory));
            } catch (FileNotFoundException e) {
                inputFileHistory = "/data/DATA/tony/app/".concat((String) conf.get(OS_prefix.concat("linear-history-file")));
                in = new BufferedReader(new FileReader(inputFileHistory));
            }

            String line;
            int counter = 0;
            int batchCounter = 0;
            int BATCH_LEN = 10000;//A batch fieldSize of 1000 to 10000 is usually OK
            Statement stmt;
            StringBuilder builder = new StringBuilder();

            //log.info(Utilities.getTimeStamp() + " : Loading history data");
            while ((line = in.readLine()) != null) {


                String[] fields = line.split(" ");
                fields[0] = fields[0].substring(2);
                fields[3] = fields[3].substring(0, fields[3].length() - 1);

                //historyEvtList.add(new HistoryEvent(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3])));
                this.dataStore.storeToll(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //log.info(Utilities.getTimeStamp() + " : Done Loading history data");
        //Just notfy this to the input input_event injector so that it can start the data emission process
//        try {
//            PrintWriter writer = new PrintWriter("done.txt", "UTF-8");
//            writer.println("\n");
//            writer.flush();
//            writer.relax_reset();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }

    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//      not in sue
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {

        int bound = in.length;
        final long bid = in.getBID();

        for (int i = 0; i < bound; i++) {


//			DailyExpenditureRequest exp = new DailyExpenditureRequest();
//			Collections.addAll(exp, in.getMsg(i).getValue(0));
            DailyExpenditureRequest exp = (DailyExpenditureRequest) in.getMsg(i).getValue(0);
            //.getValueByField(LRTopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME);
            int vehicleIdentifier = exp.getVid();
//			Object[] values;
            Integer toll = this.dataStore.retrieveToll(exp.getXWay(), exp.getDay(), vehicleIdentifier);
            if (toll != null) {
                LOG.trace("ExpenditureRequest: found vehicle identifier " + vehicleIdentifier);

                // //LOG.DEBUG("3, %d, %d, %d, %d", exp.getTime(), exp.getTimer().getOffset(), exp.getQueryIdentifier(),
                // toll);

//				values = new Object[]{AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, exp.getTime(), exp.getQid(), toll};
                this.collector.emit(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID, bid, AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, exp.getTime(), exp.getQid(), toll);
            } else {
//				values = new Object[]{AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, exp.getTime(), exp.getQid(),
//						Constants.INITIAL_TOLL};
                this.collector.emit(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID, bid, AbstractLRBTuple.DAILY_EXPENDITURE_REQUEST, exp.getTime(), exp.getQid(), Constants.INITIAL_TOLL);

            }
//			this.collector.emit(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID, bid, values);
//        if (stat != null) stat.end_measure();
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID,
                new Fields(LRTopologyControl.DAILY_EXPEDITURE_REQUEST_FIELD_NAME,
                        LRTopologyControl.TIME_FIELD_NAME, LRTopologyControl.QUERY_ID_FIELD_NAME, LRTopologyControl.TOLL_FIELD_NAME));
    }

}
