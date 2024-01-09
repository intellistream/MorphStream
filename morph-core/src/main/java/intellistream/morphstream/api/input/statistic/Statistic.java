package intellistream.morphstream.api.input.statistic;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.util.*;

import static intellistream.morphstream.util.PrintTable.printAlignedBorderedTable;

public class Statistic {
    public int shuffleType = 0;
    private Random random = new Random();
    private int delta;
    private int totalEvents = 0;
    public HashMap<Integer, InputStatistic> workerIdToInputStatisticMap = new HashMap<>();
    public OwnershipTable ownershipTable = new OwnershipTable();
    private List<Integer> tempVotes = new ArrayList<>();
    private Map<Integer, Integer> tempVoteCount = new HashMap<>();
    public Statistic(int workerNum, int shuffleType) {
        for (int i = 0; i < workerNum; i++) {
            workerIdToInputStatisticMap.put(i, new InputStatistic(i));
        }
        this.shuffleType = shuffleType;
        delta = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS") / workerNum;
    }
    public synchronized int add(List<String> keys) {
        int targetWorkerId = getTargetWorkerId(keys);
        workerIdToInputStatisticMap.get(targetWorkerId).add(keys);
        totalEvents ++;
        return targetWorkerId;
    }

    public OwnershipTable getOwnershipTable() {
        for(Map.Entry<String, Integer> ownership : this.ownershipTable.entrySet()) {
            int value = 0;
            int max = Integer.MIN_VALUE;
            for (InputStatistic inputStatistic : workerIdToInputStatisticMap.values()) {
                int number = inputStatistic.getNumber(ownership.getKey());
                if (number > max) {
                    max = number;
                    value = inputStatistic.workerId;
                }
            }
            ownership.setValue(value);
        }
        return ownershipTable;
    }

    public void display() {
        getOwnershipTable();
        ownershipTable.display();
        String[] headers = {"workerId", "totalEvents", "totalKeys", "totalOperations", "averageOperationsPerKey", "maxOperationsPerKey", "withOwnership (%)", "withoutOwnership (%)"};
        String[][] data = new String[workerIdToInputStatisticMap.size()][8];
        for (Map.Entry<Integer, InputStatistic> entry : workerIdToInputStatisticMap.entrySet()) {
            entry.getValue().display(data, this.ownershipTable);
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
            case 0://Sort
                getVotesSort(keys);
                break;
            case 1://Random
                getVotesRandom(keys);
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
            if (!this.ownershipTable.containsKey(key))
                this.ownershipTable.put(key, targetWorkerId);
        }
    }
    private void getVotesRandom(List<String> keys) {
        int targetWorkerId = random.nextInt(workerIdToInputStatisticMap.size());
        for (String key : keys) {
            this.tempVotes.add(targetWorkerId);
            if (!this.ownershipTable.containsKey(key))
                this.ownershipTable.put(key, targetWorkerId);
        }
    }
    private void getVotesPartition(List<String> keys) {
        int targetWorkerId = Integer.parseInt(keys.get(0)) / delta;
        for (String key : keys) {
            this.tempVotes.add(targetWorkerId);
            if (!this.ownershipTable.containsKey(key))
                this.ownershipTable.put(key, targetWorkerId);
        }
    }
    private void getVotesOptimized(List<String> keys) {
        for (String key : keys) {
            int targetWorkerId = 0;
            int min = Integer.MAX_VALUE;
            for (InputStatistic inputStatistic : workerIdToInputStatisticMap.values()) {
                int totalEvents = inputStatistic.totalEvents;
                if (totalEvents < min) {
                    min = totalEvents;
                    targetWorkerId = inputStatistic.workerId;
                }
            }
            int max = 0;
            for (InputStatistic inputStatistic : workerIdToInputStatisticMap.values()) {
                int number = inputStatistic.getNumber(key);
                if (number > max) {
                    max = number;
                    targetWorkerId = inputStatistic.workerId;
                }
            }
            if (!this.ownershipTable.containsKey(key))
                this.ownershipTable.put(key, targetWorkerId);
            this.tempVotes.add(targetWorkerId);
        }
    }
}
