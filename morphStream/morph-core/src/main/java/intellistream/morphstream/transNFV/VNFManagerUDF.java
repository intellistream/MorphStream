package intellistream.morphstream.transNFV;

import communication.dao.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.util.Arrays;

/** A class to store all VNF stateful events, and all VNF UDFs */

//TODO: Submit pattern data to monitor after deciding which UDF to execute
//Each VNF request includes: (int) vnfID, (int) saID
// saID = 0: read-only, saID = 1: write-only, saID = 2: read-write

public class VNFManagerUDF {

    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();

    public VNFRequest constructVNFRequest() {
        return null;
    }

    public static void simulateTask(int microseconds) {
        long startTime = System.nanoTime();
        long waitTime = microseconds * 1000L; // Convert microseconds to nanoseconds
        while (System.nanoTime() - startTime < waitTime) {
            // Busy-wait loop
        }
    }

    public static void executeUDF(VNFRequest request) {
        int vnfID = request.getVnfID();
        int saID = request.getSaID();

        simulateTask(10);

        try {
            if (vnfID == 1) {
                switch (saID) {
                    case 0:
                        FW_readAddress(request);
                        break;
                    case 1:
                    case 2:
                        FW_assignAddress(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            }
            // Handle other vnfID values if necessary
            if (vnfID == 2) {
                switch (saID) {
                    case 0:
                        NAT_readAddress(request);
                        break;
                    case 2:
                        NAT_assignAddress(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 3) {
                switch (saID) {
                    case 0:
                        LB_readAddress(request);
                        break;
                    case 2:
                        LB_assignAddress(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 4) {
                if (saID == 2) {
                    Trojan_Detector_Function(request);
                } else {
                    throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 5) {
                if (saID == 2) {
                    Portscan_Detector_Function(request);
                } else {
                    throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 6) {
                switch (saID) {
                    case 0:
                        PRADS_read(request);
                        break;
                    case 1:
                        PRADS_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 7) {
                switch (saID) {
                    case 0:
                        SBC_read(request);
                        break;
                    case 1:
                        SBC_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 8) {
                switch (saID) {
                    case 0:
                        IPS_read(request);
                        break;
                    case 2:
                        IPS_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 9) {
                if (saID == 2) {
                    Squid_write(request);
                } else {
                    throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 10) {
                switch (saID) {
                    case 0:
                        ATS_read(request);
                        break;
                    case 2:
                        ATS_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            } else if (vnfID == 11) {
                switch (saID) {
                    case 0:
                        ATS_read(request);
                        break;
                    case 1:
                    case 2:
                        ATS_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            }

        } catch (DatabaseException e) {
            e.printStackTrace();
        }

    }

    /** Firewall vnfID_1 */
    private static void FW_readAddress(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();
    }

    private static void FW_assignAddress(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();

        int chosenAddress = readValue + 1; // TODO: Simulated address assignment

        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(chosenAddress);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
    }

    /** NAT vnfID_2 */
    private static void NAT_readAddress(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();
    }

    private static void NAT_assignAddress(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();

        int chosenAddress = readValue + 1; // TODO: Simulated address assignment

        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(chosenAddress);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
    }

    /** LB vnfID_3 */
    private static void LB_readAddress(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();
    }

    private static void LB_assignAddress(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        int[] readValues = new int[10];
        for (int i = 0; i < 10; i++) { // TODO: Hardcoded address range
            TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
            SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
            int readValue = readRecord.getValues().get(1).getInt();
            readValues[i] = readValue;
        }
        int chosenAddress = Arrays.stream(readValues).min().getAsInt();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(chosenAddress);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
    }

    /** Trojan Detector vnfID_4 */ //TODO: Double check the logic
    private static void Trojan_Detector_Function(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        int payload = request.getSaID(); // TODO: Hardcoded payload to distinguish SSH, FTP, etc.
        if (payload == 1) {
            maliciousLevel += 1;
            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(maliciousLevel);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
        }
    }

    /** Portscan Detector vnfID_5 */
    private static void Portscan_Detector_Function(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        int payload = request.getSaID(); // TODO: Hardcoded payload to distinguish SSH, FTP, etc.
        if (payload == 1) {
            maliciousLevel += 1;
            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(maliciousLevel);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
        }
    }

    /** PRADS vnfID_6 */
    private static void PRADS_read(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        int payload = request.getSaID();
    }

    private static void PRADS_write(VNFRequest request) throws DatabaseException { // saID = 1, write-only
        assert request.getSaID() == 1;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
    }

    /** Session Border Controller vnfID_7 */
    private static void SBC_read(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
    }

    private static void SBC_write(VNFRequest request) throws DatabaseException { // saID = 1, write-only
        assert request.getSaID() == 1;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
    }

    /** IPS vnfID_8 */
    private static void IPS_read(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
    }

    private static void IPS_write(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
    }

    /** Squid Caching Proxy vnfID_9 */
    private static void Squid_write(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
    }

    /** Adaptive Traffic Shaper vnfID_10 */
    private static void ATS_read(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
    }

    private static void ATS_write(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
    }

}
