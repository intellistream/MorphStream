package intellistream.morphstream.engine.txn.durability.logging.LoggingEntry;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType;
import intellistream.morphstream.engine.txn.durability.struct.Logging.LVCLog;
import intellistream.morphstream.engine.db.storage.TableRecord;

import java.util.concurrent.ConcurrentSkipListSet;

public class LVLogRecord {
    public int partitionId;
    public int allocatedLSN = 0;
    public ConcurrentSkipListSet<LVCLog> logs = new ConcurrentSkipListSet<>();

    public LVLogRecord(int partitionId) {
        this.partitionId = partitionId;
    }

    public void addLog(LVCLog log, TableRecord tableRecord, int parallelNum, TableRecord[] conditionTableRecords) {
        int[] LVs = new int[parallelNum];
        for (int i = 0; i < parallelNum; i++) {
            LVs[i] = 0;
        }
        log.setLSN(allocatedLSN);//Must be set before updateReadLv or updateWriteLv
        LVs[partitionId] = allocatedLSN++;
        if (log.accessType == AccessType.READ_ONLY) {
            int[] writeLVs = tableRecord.content_.getWriteLVs();
            LVs = elemWiseMax(LVs, writeLVs);
            tableRecord.content_.updateReadLv(allocatedLSN, partitionId);
        } else if (log.accessType == AccessType.WRITE_ONLY || log.accessType == AccessType.READ_WRITE) {
            int[] comparedLVs = elemWiseMax(tableRecord.content_.getReadLVs(), tableRecord.content_.getWriteLVs());
            LVs = elemWiseMax(LVs, comparedLVs);
            tableRecord.content_.updateWriteLv(allocatedLSN, partitionId);
        } else if (log.accessType == AccessType.READ_WRITE_COND) {//Transaction event
            for (TableRecord conditionTableRecord : conditionTableRecords) {
                if (conditionTableRecord.equals(tableRecord))
                    continue;
                LVs = elemWiseMax(LVs, conditionTableRecord.content_.getWriteLVs());
                conditionTableRecord.content_.updateReadLv(allocatedLSN, partitionId);
            }
            int[] comparedLVs = elemWiseMax(tableRecord.content_.getReadLVs(), tableRecord.content_.getWriteLVs());
            LVs = elemWiseMax(LVs, comparedLVs);
            tableRecord.content_.updateWriteLv(allocatedLSN, partitionId);
        } else if (log.accessType == AccessType.READ_WRITE_COND_READN) {//Grep&Sum event
            for (TableRecord conditionTableRecord : conditionTableRecords) {
                if (conditionTableRecord.equals(tableRecord))
                    continue;
                LVs = elemWiseMax(LVs, conditionTableRecord.content_.getWriteLVs());
                conditionTableRecord.content_.updateReadLv(allocatedLSN, partitionId);
            }
            int[] comparedLVs = elemWiseMax(tableRecord.content_.getReadLVs(), tableRecord.content_.getWriteLVs());
            LVs = elemWiseMax(LVs, comparedLVs);
            tableRecord.content_.updateWriteLv(allocatedLSN, partitionId);
            tableRecord.content_.updateReadLv(allocatedLSN, partitionId);
        } else if (log.accessType == AccessType.READ_WRITE_READ) {
            int[] comparedLVs = elemWiseMax(tableRecord.content_.getReadLVs(), tableRecord.content_.getWriteLVs());
            LVs = elemWiseMax(LVs, comparedLVs);
            tableRecord.content_.updateWriteLv(allocatedLSN, partitionId);
            tableRecord.content_.updateReadLv(allocatedLSN, partitionId);
        }
        log.setLVs(LVs);
        logs.add(log);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        //   IOUtils.println("Partition " + partitionId + " has " + logs.size() + " logs");
        for (LVCLog log : logs) {
            stringBuilder.append(log.toString()).append(" ");
        }
        return stringBuilder.toString();
    }

    public void clean() {
        logs.clear();
    }

    public int[] elemWiseMax(int[] readLV, int[] writeLV) {
        int[] max = new int[readLV.length];
        for (int i = 0; i < readLV.length; i++) {
            max[i] = Math.max(readLV[i], writeLV[i]);
        }
        return max;
    }
}
