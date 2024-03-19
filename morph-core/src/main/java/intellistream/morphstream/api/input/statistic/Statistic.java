package intellistream.morphstream.api.input.statistic;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static intellistream.morphstream.util.PrintTable.printAlignedBorderedTable;

public class Statistic {
    public int shuffleType = 0;
    private Random random = new Random();
    private int delta;
    private int totalEvents = 0;
    public HashMap<Integer, InputStatistic> workerIdToInputStatisticMap = new HashMap<>();
    public DriverSideOwnershipTable driverSideOwnershipTable;
    private final List<Integer> tempVotes = new ArrayList<>();
    private final Map<Integer, Integer> tempVoteCount = new HashMap<>();
    private final ConcurrentLinkedQueue<String> tempKeys = new ConcurrentLinkedQueue();
    public Statistic(int workerNum, int shuffleType) {
        for (int i = 0; i < workerNum; i++) {
            workerIdToInputStatisticMap.put(i, new InputStatistic(i));
        }
        this.driverSideOwnershipTable = new DriverSideOwnershipTable(workerNum);
        this.shuffleType = shuffleType;
        delta = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS") / workerNum;
    }
    public synchronized int add(List<String> keys) {
        int targetWorkerId = getTargetWorkerId(keys);
        workerIdToInputStatisticMap.get(targetWorkerId).add(keys);
        totalEvents ++;
        return targetWorkerId;
    }

    public DriverSideOwnershipTable getOwnershipTable() {
        for(String key : this.tempKeys) {
            int value = 0;
            int max = Integer.MIN_VALUE;
            for (InputStatistic inputStatistic : workerIdToInputStatisticMap.values()) {
                int number = inputStatistic.getNumber(key);
                if (number > max) {
                    max = number;
                    value = inputStatistic.workerId;
                }
            }
            driverSideOwnershipTable.put(key, value);
        }
        return driverSideOwnershipTable;
    }

    public void display() {
        getOwnershipTable();
        driverSideOwnershipTable.display();
        String[] headers = {"workerId", "totalEvents", "totalKeys", "totalOperations", "averageOperationsPerKey", "maxOperationsPerKey", "withOwnership (%)", "withoutOwnership (%)"};
        String[][] data = new String[workerIdToInputStatisticMap.size()][8];
        for (Map.Entry<Integer, InputStatistic> entry : workerIdToInputStatisticMap.entrySet()) {
            entry.getValue().display(data, this.driverSideOwnershipTable);
        }
        printAlignedBorderedTable(headers, data);
    }
    private int getTargetWorkerId(List<String> keys) {
        getVotes(keys);
        getVoteCount();
        return findWinner();
    }
    private void getVotes(List<String> keys){
        switch (shuffleType) {
            case 0://Random
                getVotesRandom(keys);
                break;
            case 1://Sort
                getVotesSort(keys);
                break;
            case 2://Partition
                getVotesPartition(keys);
                break;
            case 3://Optimized
                getVotesOptimized(keys);
                break;
            default:
                throw new RuntimeException("Wrong shuffle type!");
        }
    }
    private void getVoteCount() {
        for (Integer vote : this.tempVotes) {
            if (tempVoteCount.containsKey(vote)) {
                tempVoteCount.put(vote, tempVoteCount.get(vote) + 1);
            } else {
                tempVoteCount.put(vote, 1);
            }
        }
    }
    private int findWinner() {
        int winner = 0;
        int max = Integer.MIN_VALUE;
        for (Map.Entry<Integer, Integer> entry : this.tempVoteCount.entrySet()) {
            if (entry.getValue() > max) {
                max = entry.getValue();
                winner = entry.getKey();
            }
        }
        this.tempVotes.clear();
        this.tempVoteCount.clear();
        return winner;
    }
    private void getVotesSort(List<String> keys) {
        int targetWorkerId = totalEvents % workerIdToInputStatisticMap.size();
        for (String key : keys) {
            this.tempVotes.add(targetWorkerId);
            if (!this.tempKeys.contains(key)) {
                this.tempKeys.add(key);
            }
        }
    }
    private void getVotesRandom(List<String> keys) {
        int targetWorkerId = random.nextInt(workerIdToInputStatisticMap.size());
        for (String key : keys) {
            this.tempVotes.add(targetWorkerId);
            if (!this.tempKeys.contains(key)) {
                this.tempKeys.add(key);
            }
        }
    }
    private void getVotesPartition(List<String> keys) {
        int targetWorkerId = Integer.parseInt(keys.get(0)) / delta;
        for (String key : keys) {
            this.tempVotes.add(targetWorkerId);
            if (!this.tempKeys.contains(key)) {
                this.tempKeys.add(key);
            }
        }
    }
    private void getVotesOptimized(List<String> keys) {
        for (String key : keys) {
            HashMap<Integer, Integer> totalEventsToWorkerIdMap = new HashMap<>();
            HashMap<Integer, Integer> totalKeysToWorkerIdMap = new HashMap<>();
            for (InputStatistic inputStatistic : workerIdToInputStatisticMap.values()) {
                totalEventsToWorkerIdMap.put(inputStatistic.workerId, inputStatistic.totalEvents);
                totalKeysToWorkerIdMap.put(inputStatistic.workerId, inputStatistic.getNumber(key));
            }

            HashMap<Integer, Double> totalEventsToScoreMap = Utils.assignLowScores(totalEventsToWorkerIdMap);//small get high score
            HashMap<Integer, Double> totalKeysToScoreMap = Utils.assignHighScores(totalKeysToWorkerIdMap);//big get high score

            int targetWorkerId = Utils.findHighestScoreKey(totalEventsToScoreMap, totalKeysToScoreMap, 0.5, 0.5);

            if (!this.tempKeys.contains(key)) {
                this.tempKeys.add(key);
            }
            this.tempVotes.add(targetWorkerId);
        }
    }

    public void clear() {
        //TODO: How to use the information in the ownership table
        this.tempKeys.clear();
    }
}
