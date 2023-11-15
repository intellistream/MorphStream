package commonStorage;

import java.util.HashMap;

public class TxnTemplates {
    /**
     * For each StateAccess, saDataNameToIndex maps name to index in saData array
     * "<stateObjName>": <index> *N
     * "<perEventValueName>": <index> *N
     */
    public static HashMap<String, HashMap<String, Integer>> saDataNameToIndex = new HashMap<>();
    public static HashMap<String, String[]> sharedOperatorTemplates = new HashMap<>(); //op -> txns
    public static HashMap<String, String[]> sharedTxnTemplates = new HashMap<>(); //txn -> state accesses
    /**
     * Maps saID to its templates
     * Each 4-element substring of String[] template describes one StateAccessObject:
     * 0: table name
     * 1: key index (which key in input pkt)
     * 2: field index (which field in table)
     * 3: state access type (READ/WRITE)
     */
    public static HashMap<String, String[]> sharedSATemplates = new HashMap<>(); //state access -> state access objects
}
