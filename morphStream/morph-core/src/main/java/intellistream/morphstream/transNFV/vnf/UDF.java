package intellistream.morphstream.transNFV.vnf;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.util.Arrays;

/** A class to store all VNF stateful events, and all VNF UDFs */
public class UDF {

    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int udfComplexity = MorphStreamEnv.get().configuration().getInt("udfComplexity");

    public static void executeUDF(VNFRequest request) {
        int vnfID = request.getVnfID();
        int saID = request.getSaID();
        String type = request.getType();

        try {
            if (vnfID == 1) {
                switch (type) {
                    case "Read":
                    case "Write":
                        FW_readAddress(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 2) {
                switch (type) {
                    case "Read":
                        NAT_readAddress(request);
                        break;
                    case "Write":
                        NAT_assignAddress(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 3) {
                switch (type) {
                    case "Read":
                        LB_readAddress(request);
                        break;
                    case "Write":
                        LB_assignAddress(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 4) {
                switch (type) {
                    case "Read":
                    case "Write":
                        Trojan_Detector_Function(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 5) {
                switch (type) {
                    case "Read":
                    case "Write":
                        Portscan_Detector_Function(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 6) {
                switch (type) {
                    case "Read":
                        PRADS_read(request);
                        break;
                    case "Write":
                        PRADS_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 7) {
                switch (type) {
                    case "Read":
                        SBC_read(request);
                        break;
                    case "Write":
                        SBC_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 8) {
                switch (type) {
                    case "Read":
                        IPS_read(request);
                        break;
                    case "Write":
                        IPS_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 9) {
                switch (type) {
                    case "Read":
                    case "Write":
                        Squid_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 10) {
                switch (type) {
                    case "read":
                        ATS_read(request);
                        break;
                    case "Write":
                        ATS_write(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported type value: " + type);
                }
            } else if (vnfID == 11) {
                switch (saID) {
                    case 0:
                    case 1:
                    case 2:
                        TEST_udf(request);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported saID value: " + saID);
                }
            }

        } catch (DatabaseException e) {
            e.printStackTrace();
        }

    }



    /** Test VNF vnfID_11 */
    private static void TEST_udf(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        timeout(udfComplexity);
    }

    /** Firewall vnfID_1 */
    private static void FW_readAddress(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();

        timeout(udfComplexity);
    }

    /** NAT vnfID_2 */
    private static void NAT_readAddress(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();

        timeout(udfComplexity);
    }

    private static void NAT_assignAddress(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();
        int chosenAddress = readValue + 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(chosenAddress);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        timeout(udfComplexity);
    }

    /** LB vnfID_3 */
    private static void LB_readAddress(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int readValue = readRecord.getValues().get(1).getInt();

        timeout(udfComplexity);
    }

    private static void LB_assignAddress(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        int[] readValues = new int[10];
        for (int i = 0; i < 10; i++) {
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

        timeout(udfComplexity);
    }

    /** Trojan Detector vnfID_4 */
    private static void Trojan_Detector_Function(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        int instanceID = request.getInstanceID();
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        int payload = request.getSaID();
        if (payload == 1) {
            maliciousLevel += 1;
            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(maliciousLevel);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
        }

        timeout(udfComplexity);
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
        int payload = request.getSaID();
        if (payload == 1) {
            maliciousLevel += 1;
            SchemaRecord tempo_record = new SchemaRecord(readRecord);
            tempo_record.getValues().get(1).setInt(maliciousLevel);
            tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
        }

        timeout(udfComplexity);
    }

    /** PRADS vnfID_6 */
    private static void PRADS_read(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        int payload = request.getSaID();

        timeout(udfComplexity);
    }

    private static void PRADS_write(VNFRequest request) throws DatabaseException { // saID = 1, write-only
        assert request.getSaID() == 1;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        timeout(udfComplexity);
    }

    /** Session Border Controller vnfID_7 */
    private static void SBC_read(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();

        timeout(udfComplexity);
    }

    private static void SBC_write(VNFRequest request) throws DatabaseException { // saID = 1, write-only
        assert request.getSaID() == 1;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        timeout(udfComplexity);
    }

    /** IPS vnfID_8 */
    private static void IPS_read(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;

        timeout(udfComplexity);
    }

    private static void IPS_write(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        timeout(udfComplexity);
    }

    /** Squid Caching Proxy vnfID_9 */
    private static void Squid_write(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);

        timeout(udfComplexity);
    }

    /** Adaptive Traffic Shaper vnfID_10 */
    private static void ATS_read(VNFRequest request) throws DatabaseException { // saID = 0, read-only
        assert request.getSaID() == 0;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;

        timeout(udfComplexity);
    }

    private static void ATS_write(VNFRequest request) throws DatabaseException { // saID = 2, read-write
        assert request.getSaID() == 2;
        int tupleID = request.getTupleID(); // target host ID
        long timeStamp = request.getCreateTime();
        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
        SchemaRecord readRecord = tableRecord.content_.readPreValues(timeStamp);
        int maliciousLevel = readRecord.getValues().get(1).getInt();
        maliciousLevel += 1;
        SchemaRecord tempo_record = new SchemaRecord(readRecord);
        tempo_record.getValues().get(1).setInt(maliciousLevel);
        tableRecord.content_.updateMultiValues(timeStamp, timeStamp, false, tempo_record);
        timeout(udfComplexity);
    }

    private static void timeout(int microseconds) {
        long startTime = System.nanoTime();
        long waitTime = microseconds * 1000L; // Convert microseconds to nanoseconds
        while (System.nanoTime() - startTime < waitTime) {
            // Busy-wait loop
        }
    }
}
