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


import application.datatype.AccountBalanceRequest;
import application.datatype.PositionReport;
import application.datatype.TollNotification;
import application.datatype.util.LRTopologyControl;
import application.bolts.lr.model.AccountBalance;
import application.bolts.lr.model.VehicleAccount;
import sesame.components.context.TopologyContext;
import sesame.components.operators.base.filterBolt;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * This bolt recieves the toll values assesed by the {@link TollNotificationBolt} and answers to account balance
 * queries. Therefore it processes the streams {@link LRTopologyControl#TOLL_ASSESSMENTS_STREAM_ID} (for the former) and
 * {@link LRTopologyControl#ACCOUNT_BALANCE_REQUESTS_STREAM_ID} (for the latter)
 */
public class AccountBalanceBolt extends filterBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AccountBalanceBolt.class);
    private final PositionReport inputPositionReport = new PositionReport();
    double cnt = 0, cnt1 = 0, cnt2 = 0;
    /**
     * Contains all vehicles and the accountinformation of the current day.
     */
    private Map<Integer, VehicleAccount> allVehicles;

    /**
     * almost no input for this.
     */
    public AccountBalanceBolt() {
        super(LOG, new HashMap<>(), new HashMap<>());
//        this.input_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, 0.224);
//        this.input_selectivity.put(LRTopologyControl.TOLL_ASSESSMENTS_STREAM_ID, 0.24149251117472822);
        this.input_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, 1.0);
        this.input_selectivity.put(LRTopologyControl.TOLL_ASSESSMENTS_STREAM_ID, 1.0);
        this.output_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, 1.0);
        this.setStateful();
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        this.allVehicles = new HashMap<>();
    }

    @Override
    public synchronized void execute(Tuple in) throws InterruptedException {
//         not in use
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        int bound = in.length;
//		cnt += bound;
        for (int i = 0; i < bound; i++) {

            switch (in.getSourceStreamId(i)) {
                case LRTopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID:
                    this.getBalanceAndSend(in, i);
//            cnt1++;
                    break;
                case LRTopologyControl.TOLL_ASSESSMENTS_STREAM_ID://no TA will be send in this experiment.
                    this.updateBalance(in, i);
//            cnt2++;
                    break;
                default:
                    throw new RuntimeException(String.format("Erroneous stream subscription. Please report a bug at %s",
                            "tonyzhang19900609@gmail.com"));
            }
        }
    }


    public void display() {
        LOG.info("cnt:" + cnt + "\tcnt1:" + ":" + cnt1 + "\tcnt2:" + ":" + cnt2 + "(" + ((cnt1 + cnt2) / cnt) + ")");
    }


    private synchronized void updateBalance(Tuple tuple) {
        Integer vid = tuple.getIntegerByField(LRTopologyControl.VEHICLE_ID_FIELD_NAME);
        VehicleAccount account = this.allVehicles.get(vid);
        PositionReport pos = (PositionReport) tuple.getValueByField(LRTopologyControl.POS_REPORT_FIELD_NAME);

        if (account == null) {
            int assessedToll = 0;
            //TODO:assume it's 0 now.
            account = new VehicleAccount(assessedToll, pos.getVid(), pos.getXWay(), Long.valueOf(pos.getTime()));
            this.allVehicles.put(tuple.getIntegerByField(LRTopologyControl.VEHICLE_ID_FIELD_NAME), account);
        } else {
            account.updateToll(tuple.getIntegerByField(LRTopologyControl.TOLL_FIELD_NAME));
        }
    }


    private void updateBalance(JumboTuple tuple, int i) {

        TollNotification notification = (TollNotification) tuple.getMsg(i).getValue(0);
        Integer vid = notification.getVid();
        VehicleAccount account = this.allVehicles.get(vid);
        PositionReport pos = notification.getPos();

        if (account == null) {
            int assessedToll = 0;
            //TODO:assume it's 0 now.
            account = new VehicleAccount(assessedToll, pos.getVid(), pos.getXWay(), Long.valueOf(pos.getTime()));
            this.allVehicles.put(notification.getVid(), account);
        } else {
            account.updateToll(notification.getToll());
        }
    }

    private void getBalanceAndSend(JumboTuple in, int i) throws InterruptedException {
        final long bid = in.getBID();
        AccountBalanceRequest bal = (AccountBalanceRequest) in.getMsg(i).getValue(0);

        VehicleAccount account = this.allVehicles.get(bal.getVid());

        if (account == null) {
            LOG.trace("No account information available yet: at:" + bal.getTime() + " for request" + bal);
            AccountBalance accountBalance = new AccountBalance
                    (
                            bal.getTime(),
                            bal.getQid(),
                            0, // balance
                            0, // tollTime
                            bal.getTime()
                    );

//			cnt1++;
            this.collector.emit(LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, bid, accountBalance);
        } else {
            AccountBalance accountBalance
                    = account.getAccBalanceNotification(bal);

//			cnt2++;
            this.collector.emit(LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, bid, accountBalance);
        }
    }

    private synchronized void getBalanceAndSend(Tuple in) {
        //not in use.
    }

    public Map<Integer, VehicleAccount> getAllVehicles() {
        return this.allVehicles;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, AccountBalance.getSchema()
        );
    }

}
