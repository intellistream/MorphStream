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
package intellistream.morphstream.examples.tsp.tollprocessing.util.datatype.util;

/**
 * TODO
 *
 * @author richter
 * @author mjsax
 */
public class LRTopologyControl {
    // The identifier of the topology.
    public final static String TOPOLOGY_NAME = "Linear-Road-Benchmark";
    // spouts
    public final static String SPOUT = "spout";
    public final static String SINK = "sink";
    // helper bolts
    public final static String DISPATCHER = "DISPATCHER";
    // operators
    public final static String ACCIDENT_DETECTION_BOLT = "AD";
    public final static String ACCIDENT_NOTIFICATION_BOLT_NAME = "AN";
    public final static String COUNT_VEHICLES_BOLT = "CV";
    public final static String AVERAGE_VEHICLE_SPEED_BOLT_NAME = "AV";
    public final static String AVERAGE_SPEED_BOLT = "AS";
    public final static String LAST_AVERAGE_SPEED_BOLT_NAME = "LAS";
    public final static String TOLL_NOTIFICATION_BOLT_NAME = "TN";
    public final static String TOLL_NOTIFICATION_POS_BOLT_NAME = "TN_POS";
    public final static String TOLL_NOTIFICATION_CV_BOLT_NAME = "TN_CV";
    public final static String TOLL_NOTIFICATION_LAS_BOLT_NAME = "TN_LAS";
    // sinks
    public final static String ACCIDENT_FILE_WRITER_BOLT_NAME = "Accident-File-Writer-GeneralParserBolt";
    public final static String TOLL_NOTIFICATIONS_FILE_WRITER_BOLT_NAME = "Toll-Notifications-File-Writer-GeneralParserBolt";
    public final static String TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME = "Toll-Assessments-File-Writer-GeneralParserBolt";
    // streams
    // input
    public final static String POSITION_REPORTS_STREAM_ID = "pr";
    public final static String ACCOUNT_BALANCE_REQUESTS_STREAM_ID = "ab";
    public final static String DAILY_EXPEDITURE_REQUESTS_STREAM_ID = "de";
    public final static String TRAVEL_TIME_REQUEST_STREAM_ID = "tt";
    // output
    public final static String TOLL_NOTIFICATIONS_STREAM_ID = "tn";
    public final static String TOLL_ASSESSMENTS_STREAM_ID = "ta";
    public final static String ACCIDENTS_NOIT_STREAM_ID = "an";
    public final static String ACCOUNT_BALANCE_OUTPUT_STREAM_ID = "abo";
    public final static String DAILY_EXPEDITURE_OUTPUT_STREAM_ID = "deo";
    // internal
    public final static String ACCIDENTS_STREAM_ID = "ad";
    // public final static String ACCIDENTS_STREAM_ID2 = "acc2";
    public final static String CAR_COUNTS_STREAM_ID = "cnt";
    public final static String LAVS_STREAM_ID = "lav";
    // TODO measure_end usage
    // bolts
    public final static String ACCOUNT_BALANCE_BOLT_NAME = "AB";
    public final static String ACCOUNT_BALANCE_FILE_WRITER_BOLT_NAME = "AccountBalanceFileWriterBolt";
    public final static String DAILY_EXPEDITURE_BOLT_NAME = "DailyExpenditureBolt";
    public final static String DAILY_EXPEDITURE_FILE_WRITER_BOLT_NAME = "DailyExpeditureFileWriterBolt";
    // streams
    public final static String ACCIDENT_INFO_STREAM_ID = "AccidentInfoStream";
    public final static String LAST_AVERAGE_SPEED_STREAM_ID = "LastAverageSpeedStream";
    /*
     * The identifiers of tuple attributes.
     */
    // General (all input and output tuples)
    public final static String TYPE_FIELD_NAME = "type";
    public final static String TIMESTAMP_FIELD_NAME = "timestamp";
    // Input (all)
    public final static String VEHICLE_ID_FIELD_NAME = "vid"; // also used in some output tuples
    // Position Report
    public final static String SPEED_FIELD_NAME = "spd"; // also used in Toll Notification
    public final static String XWAY_FIELD_NAME = "xway";
    public final static String LANE_FIELD_NAME = "lane";
    public final static String DIRECTION_FIELD_NAME = "dir";
    public final static String SEGMENT_FIELD_NAME = "seg"; // also used in Accident Notification
    public final static String POSITION_FIELD_NAME = "pos";
    // Requests (all)
    public final static String QUERY_ID_FIELD_NAME = "qid";
    // Daily Expenditure Request
    public final static String DAY_FIELD_NAME = "day";
    // Travel Time Request
    public final static String START_SEGMENT_FIELD_NAME = "s-init";
    public final static String END_SEGMENT_FIELD_NAME = "s-end";
    public final static String DAY_OF_WEEK_FIELD_NAME = "dow";
    public final static String TIME_OF_DAY_FIELD_NAME = "tod";
    // Output (all)
    public final static String EMIT_FIELD_NAME = "emit";
    // Accident Notification
    // re-uses SEGMENT_FIELD_NAME and VEHICLE_ID_FIELD_NAME from PositionReport
    // Toll Notification
    // re-uses SPEED_FIELD_NAME from PositionReport
    public final static String TOLL_FIELD_NAME = "toll";
    // internal
    public final static String AVERAGE_VEHICLE_SPEED_FIELD_NAME = "avgvs";
    public final static String AVERAGE_SPEED_FIELD_NAME = "avgs";
    public final static String CAR_COUNT_FIELD_NAME = "cnt";
    // TODO measure_end if needed
    public final static String POS_REPORT_FIELD_NAME = "PosReport";
    public final static String TOLL_NOTIFICATION_FIELD_NAME = "tollnotification";
    public final static String ACCOUNT_BALANCE_REQUEST_FIELD_NAME = "AccBalRequests";
    public final static String DAILY_EXPEDITURE_REQUEST_FIELD_NAME = "DaiExpRequests";
    public final static String TRAVEL_TIME_REQUEST_FIELD_NAME = "TTEstRequests";
    public final static String LAST_AVERAGE_SPEED_FIELD_NAME = "lav";
    public final static String MINUTE_FIELD_NAME = "minute";
    public final static String NUMBER_OF_VEHICLES_FIELD_NAME = "nov"; // @TODO: is that maybe the same as CAR_COUNT?
    public final static String EXPEDITURE_NOTIFICATION_FIELD_NAME = "expenditurenotification";
    public final static String ACCIDENT_NOTIFICATION_FIELD_NAME = "accnotification";
    public final static String TUPLE_FIELD_NAME = "tuple";
    public final static String ACCIDENT_INFO_FIELD_NAME = "accidentInfo";
    public final static String TIME_FIELD_NAME = "StormTimer";
    public final static String BALANCE_NOTIFICATION_REQUESTS_FIELD_NAME = "balancenotification";

    private LRTopologyControl() {
    }

    public interface Field {
        String PositionReport = "PositionReport";
        String PositionReport_Key = "PositionReport_Key";
        String AccountBalanceRequest = "AccountBalanceRequest";
        String DailyExpenditureRequest = "DailyExpenditureRequest";
        String TravelTimeRequest = "TravelTimeRequest";
        String AvgVehicleSpeedTuple = "AvgVehicleSpeedTuple";
        String AvgVehicleSpeedTuple_Key = "AvgVehicleSpeedTuple_Key";
        String LavTuple = "LavTuple";
        String AccidentTuple = "AccidentTuple";
        String TOLL_NOTIFICATIONS_STREAM = "TOLL_NOTIFICATIONS_STREAM";
        String TOLL_ASSESSMENTS_STREAM = "TOLL_ASSESSMENTS_STREAM";
        String CountTuple = "CountTuple";
        String AccidentNotification = "AccidentNotification";
        String AccountBalance = "AccountBalance";
        String DAILY_EXPEDITURE_OUTPUT_STREAM = "DAILY_EXPEDITURE_OUTPUT_STREAM";
    }
}
