package benchmark;

import common.param.TxnEvent;

import java.util.ArrayList;

public class DataHolder {
    public static ArrayList<TxnEvent> events = new ArrayList<>();
    public static ArrayList<TxnEvent> transferEvents = new ArrayList<>();
    public static ArrayList<TxnEvent> depositEvents = new ArrayList<>();
}