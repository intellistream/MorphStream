package utils;
public class PartitionHelper {
    private static int partition_interval;
    private static int partitions;
    public static int getPartitions() {
        return partitions;
    }
    public static int getPartition_interval() {
        return partition_interval;
    }
    /**
     * Each thread owns part of the data.
     *
     * @param partition_interval
     * @param partitions
     */
    public static void setPartition_interval(int partition_interval, int partitions) {
        PartitionHelper.partition_interval = partition_interval;
        PartitionHelper.partitions = partitions;
    }
    public static int key_to_partition(int key) {
        return (int) Math.floor(key / partition_interval);
    }
    public static int key_to_partition(String key) {
        return (int) Math.floor(Integer.parseInt(key) / partition_interval);
    }
}
