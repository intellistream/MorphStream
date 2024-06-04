package intellistream.morphstream.api.input.statistic;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import lombok.Getter;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static intellistream.morphstream.util.PrintTable.printAlignedBorderedTable;

public class Statistic {
    public int shuffleType = 0;
    public int workNum;
    private Random random = new Random();
    private int delta;
    private int totalEvents = 0;
    private String[] tableNames;
    private final ConcurrentHashMap<Integer, Double> frontendIdToThroughput = new ConcurrentHashMap<>();
    @Getter
    private final DescriptiveStatistics latencyStatistics = new DescriptiveStatistics();
    private final ConcurrentHashMap<Long, Long> bidToStartTimestamp = new ConcurrentHashMap<>();
    public HashMap<Integer, InputStatistic> workerIdToInputStatisticMap = new HashMap<>();
    private final ConcurrentHashMap<Integer, List<Integer>> tempVotes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Map<Integer, Integer>> tempVoteCount = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> tempKeys = new ConcurrentHashMap<>();//Table name -> keys
    public Statistic(int workerNum, int shuffleType, String[] tableNames, int frontendNumber) {
        this.tableNames = tableNames;
        for (String tableName : tableNames) {
            tempKeys.put(tableName, new ConcurrentLinkedQueue<>());
        }
        for (int i = 0; i < workerNum; i++) {
            workerIdToInputStatisticMap.put(i, new InputStatistic(i, tableNames));
        }
        for (int i = 0; i < frontendNumber; i++) {
           tempVotes.put(i, new ArrayList<>());
           tempVoteCount.put(i, new HashMap<>());
        }
        this.shuffleType = shuffleType;
        this.workNum = workerNum;
        delta = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS") / workerNum;
    }
    public int add(HashMap<String, List<String>> keyMap, int frontendId) {
        int targetWorkerId = getTargetWorkerId(keyMap, frontendId);
        for (Map.Entry<String, List<String>> entry : keyMap.entrySet()) {
            workerIdToInputStatisticMap.get(targetWorkerId).add(entry.getValue(), entry.getKey());
        }
        return targetWorkerId;
    }

    public DriverSideOwnershipTable getOwnershipTable(String tableName) {
        DriverSideOwnershipTable driverSideOwnershipTable = new DriverSideOwnershipTable(workNum);
        for(String key : this.tempKeys.get(tableName)) {
            int id = 0;
            int max = Integer.MIN_VALUE;
            for (InputStatistic inputStatistic : workerIdToInputStatisticMap.values()) {
                int number = inputStatistic.getNumber(key, tableName);
                if (number > max) {
                    max = number;
                    id = inputStatistic.workerId;
                }
            }
            driverSideOwnershipTable.put(key, id);
        }
        return driverSideOwnershipTable;
    }

    public void display() {
        for (String tableName : tableNames) {
            DriverSideOwnershipTable driverSideOwnershipTable = getOwnershipTable(tableName);
            driverSideOwnershipTable.display();
            String[] headers = {"workerId", "totalEvents", "totalKeys", "totalOperations", "averageOperationsPerKey", "maxOperationsPerKey", "withOwnership (%)", "withoutOwnership (%)"};
            String[][] data = new String[workerIdToInputStatisticMap.size()][8];
            for (Map.Entry<Integer, InputStatistic> entry : workerIdToInputStatisticMap.entrySet()) {
                entry.getValue().display(data, driverSideOwnershipTable);
            }
            printAlignedBorderedTable(headers, data);
        }
    }
    private int getTargetWorkerId(HashMap<String, List<String>> keyMap, int frontendId) {
        getVotes(keyMap, frontendId);
        getVoteCount(frontendId);
        return findWinner(frontendId);
    }
    private void getVotes(HashMap<String, List<String>> keyMap, int frontendId){
        switch (shuffleType) {
            case 0://Random
                getVotesRandom(keyMap, frontendId);
                break;
            case 1://Sort
                getVotesSort(keyMap, frontendId);
                break;
            case 2://Partition
                getVotesPartition(keyMap, frontendId);
                break;
            case 3://Optimized
                getVotesOptimized(keyMap, frontendId);
                break;
            default:
                throw new RuntimeException("Wrong shuffle type!");
        }
    }
    private void getVoteCount(int frontendId) {
        for (Integer vote : this.tempVotes.get(frontendId)) {
            if (tempVoteCount.get(frontendId).containsKey(vote)) {
                tempVoteCount.get(frontendId).put(vote, tempVoteCount.get(frontendId).get(vote) + 1);
            } else {
                tempVoteCount.get(frontendId).put(vote, 1);
            }
        }
    }
    private int findWinner(int frontendId) {
        int winner = 0;
        int max = Integer.MIN_VALUE;
        for (Map.Entry<Integer, Integer> entry : this.tempVoteCount.get(frontendId).entrySet()) {
            if (entry.getValue() > max) {
                max = entry.getValue();
                winner = entry.getKey();
            }
        }
        this.tempVotes.get(frontendId).clear();
        this.tempVoteCount.get(frontendId).clear();
        return winner;
    }
    private void getVotesSort(HashMap<String, List<String>> keyMap, int frontendId) {
        int targetWorkerId = totalEvents % workerIdToInputStatisticMap.size();
        for (String tableName : keyMap.keySet()) {
            for (String key : keyMap.get(tableName)) {
                this.tempVotes.get(frontendId).add(targetWorkerId);
                if (!this.tempKeys.get(tableName).contains(key)) {
                    this.tempKeys.get(tableName).add(key);
                }
            }
        }
    }
    private void getVotesRandom(HashMap<String, List<String>> keyMap, int frontendId) {
        for (String tableName : keyMap.keySet()) {
            for (String key : keyMap.get(tableName)) {
                int targetWorkerId = random.nextInt(workerIdToInputStatisticMap.size());
                this.tempVotes.get(frontendId).add(targetWorkerId);
                if (!this.tempKeys.get(tableName).contains(key)) {
                    this.tempKeys.get(tableName).add(key);
                }
            }
        }
    }
    private void getVotesPartition(HashMap<String, List<String>> keyMap, int frontendId) {
        for (String tableName : keyMap.keySet()) {
            for (String key : keyMap.get(tableName)) {
                int targetWorkerId = Integer.parseInt(key) / delta;
                this.tempVotes.get(frontendId).add(targetWorkerId);
                if (!this.tempKeys.get(tableName).contains(key)) {
                    this.tempKeys.get(tableName).add(key);
                }
            }
        }
    }
    private void getVotesOptimized(HashMap<String, List<String>> keyMap, int frontendId) {
        for (String tableName : keyMap.keySet()) {
            for (String key : keyMap.get(tableName)) {
                HashMap<Integer, Integer> totalEventsToWorkerIdMap = new HashMap<>();
                HashMap<Integer, Integer> totalKeysToWorkerIdMap = new HashMap<>();
                for (InputStatistic inputStatistic : workerIdToInputStatisticMap.values()) {
                    totalEventsToWorkerIdMap.put(inputStatistic.workerId, inputStatistic.totalEvents);
                    totalKeysToWorkerIdMap.put(inputStatistic.workerId, inputStatistic.getNumber(key, tableName));
                }

                HashMap<Integer, Double> totalEventsToScoreMap = Utils.assignHighScoresToSmall(totalEventsToWorkerIdMap);//small get high score
                HashMap<Integer, Double> totalKeysToScoreMap = Utils.assignLowScoresToSmall(totalKeysToWorkerIdMap);//big get high score

                int targetWorkerId = Utils.findHighestScoreKey(totalEventsToScoreMap, totalKeysToScoreMap, 0.5, 0.5);

                if (!this.tempKeys.get(tableName).contains(key)) {
                    this.tempKeys.get(tableName).add(key);
                }
                this.tempVotes.get(frontendId).add(targetWorkerId);
            }
        }
    }

    public void clear() {
        //TODO: How to use the information in the ownership table
        this.tempKeys.clear();
    }
    public void addThroughput(int frontendId, double throughput) {
        frontendIdToThroughput.put(frontendId, throughput);
    }
    public void addStartTimestamp(long bid, long startTimestamp) {
        bidToStartTimestamp.put(bid, startTimestamp);
    }
    public void addLatency(long bid, long endTimestamp) {
        long startTimestamp = bidToStartTimestamp.get(bid);
        latencyStatistics.addValue((endTimestamp - startTimestamp) / 1E6);
    }
    public double getThroughput() {
        double sum = 0;
        for (Double throughput : frontendIdToThroughput.values()) {
            sum += throughput;
        }
        return sum;
    }
    public double getLatency() {
        return latencyStatistics.getMean();
    }
    public double getLatency(double percentile) {
        return latencyStatistics.getPercentile(percentile);
    }

}
