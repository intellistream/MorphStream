package cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cli.CliFrontend.getDoubleField;
import static cli.CliFrontend.setDoubleField;
import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class FastSLClient {
    private static final Logger log = LoggerFactory.getLogger(FastSLClient.class);

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
    public boolean execute_txn_udf(String saID, String[] txnData) {
        if (saID == "srcTransfer") {
            double srcBalance = getDoubleField("srcAccountBalance", txnData);
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

    public static void main(String[] args) throws Exception {
        CliFrontend fastSLClient = new CliFrontend("FastSLClient");
        fastSLClient.loadConfig();

        fastSLClient.registerStateObject("srcAccountState", "accounts", 0, 1, "WRITE");
        fastSLClient.registerStateObject("destAccountState", "accounts", 0, 1, "WRITE");
        String[] srcTransferStateObjs = {"srcAccountState"};
        String[] destTransferStateObjs = {"srcAccountState", "destAccountState"};

        fastSLClient.registerStateAccess("srcTransfer", srcTransferStateObjs, null, "WRITE");
        fastSLClient.registerStateAccess("destTransfer", destTransferStateObjs, null, "WRITE");
        fastSLClient.registerStateAccess("deposit", srcTransferStateObjs, null, "WRITE");
        String[] transferStateAccessIDs = {"srcTransfer", "destTransfer"};
        String[] depositStateAccessIDs = {"deposit"};

        fastSLClient.registerTxn("transfer", transferStateAccessIDs);
        fastSLClient.registerTxn("deposit", depositStateAccessIDs);
        String[] txnIDs = {"transfer", "deposit"};

        fastSLClient.registerOperator("fastSLClient", txnIDs, 0, 4);

        fastSLClient.start();
    }
}
