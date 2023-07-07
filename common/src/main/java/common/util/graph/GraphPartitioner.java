package common.util.graph;

import java.util.*;

// Greedy algorithm
// The time complexity is O(n log n), where n is the number of nodes
public class GraphPartitioner {
    private int nodeSize;  // Node
    private List<Edge> edges;  // Edge
    int[] nodeWeights;  // Node weight
    private final int[][] partitionEdgesWeight;// Edge weight between partitions
    private int[] partitionWeights;  // Partition weight
    private int[][] benefits;  // the benefit to move node_i to partition_j int[i][j] Vi -> Pj
    List<List<Integer>> partitions = new ArrayList<>();// Partition
    private int partitionCount;  // Partition count
    public GraphPartitioner(int nodeSize, int[] nodeWeights, List<Edge> edges, int partitionCount) {
        this.nodeSize = nodeSize;
        this.edges = edges;
        this.nodeWeights = nodeWeights;
        this.partitionCount = partitionCount;
        this.partitionEdgesWeight = new int[partitionCount][partitionCount];
        this.partitionWeights = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new ArrayList<>());
        }
        benefits = new int[nodeSize][partitionCount];
    }

    public List<List<Integer>> run(int max_itr) {
        initPartitions();
        while (max_itr-- > 0) {
            int[] selectedBenefit = this.findMaxBenefit();
            int fromPartition = findPartition(selectedBenefit[0]);
            int targetPartition = selectedBenefit[1];
            if (benefits[fromPartition][targetPartition] > 0) {
                moveNode(fromPartition, targetPartition, selectedBenefit[0]);
                calcBenefit();
            } else {
                break;
            }
        }
        return partitions;
    }
    private void initPartitions() {
        //Partition G into a set of sub-graphs {G1,G2,. . . ,Gn} with about the same weight according to the key range
        //Workload-aware graph partitioning
        greedyPartition();
        //calcWeight();
        calcBenefit();
    }
    /**
     * Compute the edge weight between partitions
     */
    private void calcWeight() {
        for (Edge edge : this.edges) {
            int v1 = edge.getFrom();
            int v2 = edge.getTo();
            int partition1 = findPartition(v1);
            int partition2 = findPartition(v2);
            if (partition1 != partition2) {
                partitionEdgesWeight[partition1][partition2] += edge.getWeight();
                partitionEdgesWeight[partition2][partition1] += edge.getWeight();
            }
        }
    }
    private void initBenefit() {
        for (int i = 0; i < benefits.length; i++) {
            for (int j = 0; j < benefits[i].length; j++) {
                benefits[i][j] = 0;
            }
        }
    }
    /**
     * Compute the benefit to move node_i to partition_j
     */
    private void calcBenefit() {
        initBenefit();
        for (Edge edge : this.edges) {
            int v1 = edge.getFrom();
            int v2 = edge.getTo();
            int partition1 = findPartition(v1);
            int partition2 = findPartition(v2);
            for (int i = 0; i < partitionCount; i++) {
                //Move V1 to Pi
                if (i != partition1 && i == partition2) {
                    benefits[v1][i] += edge.getWeight();
                }
                if (i != partition1 && i != partition2) {
                    benefits[v1][i] -= edge.getWeight();
                }
                //Move V2 to Pi
                if (i != partition2 && i == partition1) {
                    benefits[v2][i] += edge.getWeight();
                }
                if (i != partition2 && i != partition1) {
                    benefits[v2][i] -= edge.getWeight();
                }
            }
        }
    }
    /**
     * Find the maximum benefit to move a node from one partition to another
     */
    public int[] findMaxBenefit() {
        int[] maxIndex = new int[2];
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < this.benefits.length; i++) {
            for (int j = 0; j < this.benefits[i].length; j++) {
                if (this.benefits[i][j] > max) {
                    max = this.benefits[i][j];
                    maxIndex[0] = i;
                    maxIndex[1] = j;
                }
            }
        }
        return maxIndex;
    }
    /**
     * Move a node from one partition to another
     */
    private void moveNode(int fromPartition, int targetPartition, int nodeId) {
        this.partitions.get(fromPartition).remove((Object) nodeId);
        this.partitions.get(targetPartition).add(nodeId);
    }
    /**
     * Find the partition that a node belongs to
     */
    private int findPartition(int vertex) {
        for (int i = 0; i < partitionCount; i++) {
            if (this.partitions.get(i).contains(vertex)) {
                return i;
            }
        }
        return -1;
    }
    public List<List<Integer>> getPartitions() {
        return partitions;
    }
    public void weightPartition() {
        int [][] dp = new int[partitionCount + 1][this.nodeSize + 1];
        int [][] plan = new int[partitionCount + 1][this.nodeSize + 1];
        assignTasksByDP(this.nodeWeights, partitionCount, dp, plan);
        this.partitions = getTaskAssignmentsByDP(plan);
    }
    public void greedyPartition() {
        this.partitions = allocateTasksByGreedy(this.nodeWeights, partitionCount);
    }
    public static int assignTasksByDP(int[] tasks, int n, int[][] dp, int[][] plan) {
        int[] sum = new int[tasks.length + 1];
        for (int i = 1; i <= tasks.length; i++) {
            sum[i] = sum[i - 1] + tasks[i - 1];
        }

        for (int i = 1; i <= n; i++) {
            dp[i][1] = tasks[0];
            plan[i][1] = 1;
        }

        for (int i = 2; i <= tasks.length; i++) {
            dp[1][i] = sum[i];
        }

        for (int i = 2; i <= n; i++) {
            for (int j = 2; j <= tasks.length; j++) {
                dp[i][j] = Integer.MAX_VALUE;
                for (int k = i - 1; k < j; k++) {
                    int val = Math.max(dp[i - 1][k], sum[j] - sum[k]);
                    if (val < dp[i][j]) {
                        dp[i][j] = val;
                        plan[i][j] = k + 1;
                    }
                }
            }
        }
        return dp[n][tasks.length];
    }
    public static List<List<Integer>> getTaskAssignmentsByDP(int[][] plan) {
        List<List<Integer>> result = new ArrayList<>();
        int n = plan.length - 1;
        int m = plan[0].length - 1;
        int curThread = n;
        int curTask = m;

        while (curThread > 0) {
            List<Integer> assignments = new ArrayList<>();
            for (int i = plan[curThread][curTask]; i <= curTask; i++) {
                assignments.add(i - 1);
            }
            Collections.reverse(assignments);
            result.add(assignments);
            curTask = plan[curThread][curTask] - 1;
            curThread--;
        }

        Collections.reverse(result);
        return result;
    }
    public static List<List<Integer>> allocateTasksByGreedy(int[] tasks, int n) {
        int m = tasks.length;
        List<List<Integer>> allocation = new ArrayList<>();

        // 1. 初始化分配结果
        for (int i = 0; i < n; i++) {
            allocation.add(new ArrayList<>());
        }

        // 2. 按照权重从大到小排序
        Integer[] taskIds = new Integer[m];
        for (int i = 0; i < m; i++) {
            taskIds[i] = i;
        }
        Arrays.sort(taskIds, Comparator.comparingInt(i -> -tasks[i]));

        // 3. 贪心分配任务
        int[] workloads = new int[n];
        for (int taskId : taskIds) {
            int minIndex = 0;
            for (int i = 1; i < n; i++) {
                if (workloads[i] < workloads[minIndex]) {
                    minIndex = i;
                }
            }
            allocation.get(minIndex).add(taskId);
            workloads[minIndex] += tasks[taskId];
        }

        return allocation;
    }
}
