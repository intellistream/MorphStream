package cli;

import intellistream.morphstream.api.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cli.CliFrontend.*;

public class FastSLClient extends Client {
    private static final Logger log = LoggerFactory.getLogger(FastSLClient.class);

    /**
     * saData contains:
     * 0: saID
     * 1: txnAbortFlag
     * 2: saResult
     * 3 onwards: stateObj1's field (each stateObj specifies 1 field of 1 TableRecord, assume txnData already contains retrieved field)
     * ...
     */
    public String[] execute_txn_udf(String saID, String[] saData) {
        if (saID == "srcTransfer") {
            double srcBalance = getDoubleField("srcAccountBalance", saData);
            if (srcBalance > 100) {
                setDoubleField("srcAccountBalance", srcBalance - 100, saData);
            } else {
                abortTxn(saData); //an example of abort txn at application-level
            }
        } else if (saID == "destTransfer") {
            double srcBalance = getDoubleField("srcAccountBalance", saData);
            double destBalance = getDoubleField("destAccountBalance", saData);
            if (srcBalance > 100) {
                setDoubleField("destAccountBalance", destBalance + 100, saData);
            } else {
                abortTxn(saData);
            }
        } else if (saID == "deposit") {
            double srcBalance = getDoubleField("srcAccountBalance", saData);
            setDoubleField("srcAccountBalance", srcBalance + 100, saData);
        } else {
            abortTxn(saData);
        }
        return saData;
    }

    public static void main(String[] args) throws Exception {
        CliFrontend fastSLClient = new CliFrontend("FastSLClient");
        fastSLClient.loadConfig(args);

        fastSLClient.registerStateObject("srcAccountBalance", "accounts", 0, 1, "WRITE");
        fastSLClient.registerStateObject("destAccountBalance", "accounts", 1, 1, "WRITE");
        String[] srcTransferStateObjs = {"srcAccountBalance"};
        String[] destTransferStateObjs = {"srcAccountBalance", "destAccountBalance"};

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
