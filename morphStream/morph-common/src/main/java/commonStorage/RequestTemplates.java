package commonStorage;

import java.util.HashMap;

public class RequestTemplates {
    /**
     * For each StateAccess, saDataNameToIndex maps name to index in saData array
     * "<stateObjName>": <index> *N
     * "<perEventValueName>": <index> *N
     */
    public static HashMap<String, HashMap<String, Integer>> saDataNameToIndex = new HashMap<>();
    public static HashMap<String, String[]> sharedOperatorTemplates = new HashMap<>(); //op -> txns
    public static HashMap<String, String[]> sharedTxnTemplates = new HashMap<>(); //txn -> state accesses
    /**
     * Maps stateAccessID to stateAccessTemplate
     * Key: stateAccessID, Value: stateAccessTemplate
     * Each stateAccessTemplate is a String array of:
     * --> 0: stateAccessID
     * --> 1: stateAccessType
     * --> 2: writeObjIndex (which stateObject to write in stateObjectTemplateArray)
     * --> 3 onwards: stateObjectTemplateArray, each stateObjectTemplate consists of four elements:
     * --> --> table name, key index (which key in input pkt), field index (which field in table), state access type (READ/WRITE)
     */
    public static HashMap<String, String[]> sharedSATemplates = new HashMap<>(); //state access -> state access objects
}
