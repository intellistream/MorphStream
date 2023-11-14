package cli;

import java.util.HashMap;

public class FastSLClient {

    //TODO: Initialize nameToIndex
    /**
     * nameToIndex should store all name to index mappings, including readField, writeField, per-event data, etc.
     */
    public static HashMap<String, Integer> nameToIndex = new HashMap<>(); // identifier -> index in txn byte array
    public static HashMap<String, String> stateObjTable = new HashMap<>(); // stateObjID -> stateObjTable
    public static HashMap<String, String> stateObjField = new HashMap<>(); // stateObjID -> stateObjField

    /**
     * txnData contains:
     * 0: saID
     * 1: stateObj1's field (each stateObj specifies 1 field of 1 TableRecord, assume txnData already contains retrieved field)
     * ...
     */

    /**
     * OP reads and gets condition_records from operation
     * OP stores client-desired fields into txnData following txnTemplate
     * UDF retrieves fields from txnData
     * UDF computes writeValue based on fields
     * ...
     */
    public static double getDoubleField(String stateObjID, String[] txnData) {
        int readFieldIndex = nameToIndex.get(stateObjID); // starting index of state object in txn byte array
        return Double.parseDouble(txnData[readFieldIndex]);
    }

    public static void setDoubleField(String stateObjID, double value, String[] txnData) {
        int writeFieldIndex = nameToIndex.get(stateObjID); // starting index of state object in txn byte array
        txnData[writeFieldIndex] = Double.toString(value);
    }


    public boolean execute_txn_udf(String saID, String[] txnData) {

        if (saID == "srcTransfer") {
            double srcBalance = getDoubleField("srcAccountBalance", txnData);
            double destBalance = getDoubleField("destAccountBalance", txnData);
            if (srcBalance > 100) {
                setDoubleField("srcAccountBalance", srcBalance - 100, txnData);
                return true;
            } else {
                return false;
            }
        } else if (saID == "destTransfer") {
            double srcBalance = getDoubleField("srcAccountBalance", txnData);
            double destBalance = getDoubleField("destAccountBalance", txnData);
            if (srcBalance > 100) {
                setDoubleField("destAccountBalance", destBalance + 100, txnData);
                return true;
            } else {
                return false;
            }
        } else if (saID == "deposit") {
            double srcBalance = getDoubleField("srcAccountBalance", txnData);
            setDoubleField("srcAccountBalance", srcBalance + 100, txnData);
            return true;
        } else {
            return false;
        }
    }
}
