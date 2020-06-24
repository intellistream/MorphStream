package sesame.optimization.model;

import application.Platform;
import sesame.components.TopologyComponent;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.SchedulingPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by shuhaozhang on 22/9/16.
 */
public class Constraints implements Serializable {
    private static final long serialVersionUID = -8506858194283640076L;
    private static final Logger LOG = LoggerFactory.getLogger(Constraints.class);
    private final static double EPSILON = 0.00001;
    public final int allstatisfy = 15;
    private final Platform p;
    private final double unitBandwidth;
    private final int sockets;
    private final int cores;//cores per socket.
    private final double relax = 1;
    public double input_rate;
    public double relax_cpu;
    public int relax_cores;
    public double relax_memory;
    public double relax_qpi;
    private double bk_relax_cpu;
    private int bk_relax_cores;
    private double bk_relax_memory;
    private double bk_relax_qpi;

    public Constraints(int num_sockets, int numCPUs, Platform p) {
        this.p = p;
        unitBandwidth = p.cache_line;
        sockets = num_sockets;
        cores = numCPUs;
        relax_reset();
    }

    private Constraints(Constraints cons) {
        this.relax_memory = cons.relax_memory;
        this.relax_qpi = cons.relax_qpi;
        this.relax_cpu = cons.relax_cpu;
        this.relax_cores = cons.relax_cores;
        this.sockets = cons.sockets;
        this.cores = cons.cores;
        this.p = cons.p;
        unitBandwidth = p.cache_line;
    }

    /**
     * @param socket
     * @return Cycles per ns per socket.
     */
    public double Available_CPU(int socket) {

//		if (socket == 0) {
//			return 0;//disable usage of socket 0.
//		}
        return p.CLOCK_RATE * cores * relax_cpu;
    }

    public int Available_CORES(int checkSocket) {
        if (checkSocket == 0)
            return cores - 1;//core 0 is excluded.

        return cores + relax_cores;
    }

    /**
     * @param socket
     * @return Bandwidth in Bytes per ns on the given %socket.
     */
    public double Available_bandwidth(int socket) {
        return p.bandwidth_map[socket][socket] * p.bandwdith_convert * relax_memory;
    }

    private double Available_QPI_bandwidth(int si, int sj) {
        return p.bandwidth_map[si][sj] * p.bandwdith_convert * relax_qpi;
    }

    private void setRelax_cpu(double relax_cpu) {
        this.relax_cpu = relax_cpu;
    }

    private void setRelax_memory(double relax_memory) {
        this.relax_memory = relax_memory;
    }

    private void setRelax_qpi(double relax_qpi) {
        this.relax_qpi = relax_qpi;
    }

    private void setRelax_cores(int relax_cores) {
        this.relax_cores = relax_cores;
    }

    public int valid_satisfy(SchedulingPlan plan) {

        int flag = (valid_C1(plan) ? 1 : 0);

        flag += (valid_C2(plan) ? 2 : 0);

        flag += (valid_C3(plan) ? 4 : 0);

        flag += (valid_C4(plan) ? 8 : 0);

//		flag += 8; // let #core always satisfying.

        return flag;
    }

    public int satisfy(SchedulingPlan plan) {

        int flag = (C1(plan) ? 1 : 0);

        flag += (C2(plan) ? 2 : 0);

        flag += (C3(plan) ? 4 : 0);

        flag += (C4(plan) ? 8 : 0);//

//		flag += 8; //let #core always satisfying.


        return flag;
    }

    public boolean valid_check(SchedulingPlan plan) {
//		LOG.info("====measure_end plan====");
        final int satisfy = valid_satisfy(plan);
//		LOG.info("ConstraintBy by:" + constraintBy(satisfy));
//		LOG.info("\n" + this.show(plan));
        return satisfy == this.allstatisfy;
    }

    /**
     * Debug purpose.
     *
     * @param plan
     */
    public boolean check(SchedulingPlan plan) {
//		LOG.info("====measure_end plan====");
        final int satisfy = satisfy(plan);

        if (satisfy != this.allstatisfy) {
            LOG.info("ConstraintBy by:" + constraintBy(satisfy));
            LOG.info("\n" + this.show(plan));
            return false;
        }
        return true;
    }

    public int satisfy(SchedulingPlan plan, int checkSocket) {

        int flag = (C1_socket(plan, checkSocket) ? 1 : 0);

        flag += (C2_socket(plan, checkSocket) ? 2 : 0);

        flag += (C3(plan) ? 4 : 0);

        flag += (C4_socket(plan, checkSocket) ? 8 : 0);//

//		flag += 8;  //let #core always satisfying.

        return flag;
    }

    private boolean valid_C1_socket(SchedulingPlan plan, int checkSocket) {
        double total_demand = valid_CPU_demand(plan.validationMap, plan, checkSocket);

        if (Math.abs(Available_CPU(checkSocket) - total_demand) < EPSILON) {
            return true;
        }

        return Available_CPU(checkSocket) >= total_demand;
    }

    /**
     * CPU Cycles limitation
     *
     * @param plan @return
     */
    private boolean valid_C1(SchedulingPlan plan) {

        for (int i = 0; i < sockets; i++) {
            if (!valid_C1_socket(plan, i)) {
                return false;
            }
        }
        return true;
    }

    private boolean C1_socket(SchedulingPlan plan, int checkSocket) {
        double total_demand = CPU_demand(plan, checkSocket);
        if (Math.abs(Available_CPU(checkSocket) - total_demand) < EPSILON) {
            return true;
        }
        return Available_CPU(checkSocket) >= total_demand;
    }


    /**
     * CPU Cycles limitation
     *
     * @param plan
     * @return
     */
    private boolean C1(SchedulingPlan plan) {
        for (int i = 0; i < sockets; i++) {
            if (!C1_socket(plan, i)) {
                return false;
            }
        }
        return true;
    }

    private boolean valid_C4_socket(SchedulingPlan plan, int checkSocket) {
        int cores_demand = valid_cores_demand(plan.validationMap, plan, checkSocket);
        return Available_CORES(checkSocket) >= cores_demand;
    }


    /**
     * Examine if number of operators exceed number of cores in a socket.
     *
     * @param plan
     * @return
     */
    private boolean valid_C4(SchedulingPlan plan) {
        for (int i = 0; i < sockets; i++) {
            if (!valid_C4_socket(plan, i)) {
                return false;
            }
        }
        return true;
    }

    private boolean C4_socket(SchedulingPlan plan, int checkSocket) {
        int cores_demand = cores_demand(plan, checkSocket);
        return Available_CORES(checkSocket) >= cores_demand;
    }


    /**
     * Examine if number of operators exceed number of cores in a socket.
     *
     * @param plan
     * @return
     */
    private boolean C4(SchedulingPlan plan) {
        for (int i = 0; i < sockets; i++) {
            if (!C4_socket(plan, i)) {
                return false;
            }
        }
        return true;
    }


    private boolean valid_C2_socket(SchedulingPlan plan, int checkSocket) {
        double bandwidth_demand = valid_Bandwidth_demand(plan.validationMap, plan, checkSocket);
        return !(Available_bandwidth(checkSocket) < bandwidth_demand);
    }

    /**
     * Memory bandwidth limitation
     *
     * @param plan @return
     */
    private boolean valid_C2(SchedulingPlan plan) {
        for (int i = 0; i < sockets; i++) {
            if (!valid_C2_socket(plan, i)) {
                return false;
            }
        }
        return true;
    }

    private boolean C2_socket(SchedulingPlan plan, int checkSocket) {
        double bandwidth_demand = Bandwidth_demand(plan, checkSocket);
        return !(Available_bandwidth(checkSocket) < bandwidth_demand);
    }

    /**
     * Memory bandwidth limitation
     *
     * @param plan
     * @return
     */
    private boolean C2(SchedulingPlan plan) {
        for (int i = 0; i < sockets; i++) {
            if (!C2_socket(plan, i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * QPI bandwidth limitation
     *
     * @param plan @return
     */
    private boolean valid_C3(SchedulingPlan plan) {
        for (int i = 0; i < sockets; i++) {
            for (int j = 0; j < sockets; j++) {
                if (i == j) {
                    continue;
                }
                double bandwidth_demand = valid_QPI_Bandwidth_demand(plan.validationMap, plan, i, j);
                if (Available_QPI_bandwidth(i, j) < bandwidth_demand) {
//                    LOG.info("QPI bandwidth demand is too high to be allocated from socket:" + i
//                            + "\tto socket" + j);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * QPI bandwidth limitation
     *
     * @param plan
     * @return
     */
    private boolean C3(SchedulingPlan plan) {
        for (int i = 0; i < sockets; i++) {
            for (int j = 0; j < sockets; j++) {
                if (i == j) {
                    continue;
                }
                double bandwidth_demand = QPI_Bandwidth_demand(plan, i, j);
                if (Available_QPI_bandwidth(i, j) < bandwidth_demand) {
//                    LOG.info("QPI bandwidth demand is too high to be allocated from socket:" + i
//                            + "\tto socket" + j);
                    return false;
                }
            }
        }
        return true;
    }

    private double valid_CPU_demand(Map<Integer, Boolean> validationMap,
                                    SchedulingPlan plan, int socket) {
        double total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {
            if (p(plan, executor, socket) && validationMap.get(executor.getExecutorID())) {
                double executorCycles = executor.getdemandCycles(plan, true);
                // LOG.info("BasicBoltBatchExecutor:" + executor.getExecutorIDList() + "\ttakes:" + executorCycles);
                total_demand += executorCycles;
            }
        }
        return total_demand;
    }


    private double CPU_demand(SchedulingPlan plan, int socket) {
        double total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {
            if (p(plan, executor, socket)) {
                double executorCycles = executor.getdemandCycles(plan, false);
//				LOG.info("On socket:" + socket + " Executor:" + executor.getExecutorID() + "\ttakes:" + executorCycles);
                total_demand += executorCycles;
            } else {
//				LOG.info("Executor:" + executor.getExecutorID() + " is not allocated at socket:" + socket);
            }
        }
        return total_demand;
    }

    private int cores_demand(SchedulingPlan plan, int socket) {
        int total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {
            if (p(plan, executor, socket) && !executor.isVirtual()) {
                // LOG.info("BasicBoltBatchExecutor:" + executor.getExecutorIDList() + "\ttakes:" + executorCycles);
                total_demand += executor.getdemandCores();
            }
        }
        return total_demand;
    }

    private int valid_cores_demand(Map<Integer, Boolean> validationMap, SchedulingPlan plan, int socket) {
        int total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {
            if (p(plan, executor, socket) && !executor.isVirtual() && validationMap.get(executor.getExecutorID())) {
                // LOG.info("BasicBoltBatchExecutor:" + executor.getExecutorIDList() + "\ttakes:" + executorCycles);
                total_demand += executor.getdemandCores();
            }
        }
        return total_demand;
    }

    private double valid_Bandwidth_demand(Map<Integer, Boolean> validationMap
            , SchedulingPlan plan, int socket) {
        double total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {

            if (p(plan, executor, socket) && validationMap.get(executor.getExecutorID())) {
                double memConsumption = executor.getMemConsumption(plan, true);
                total_demand += memConsumption;
            }
        }
        return total_demand;
    }

    private double Bandwidth_demand(SchedulingPlan plan, int socket) {

        double total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {

            if (p(plan, executor, socket)) {
                double memConsumption = executor.getMemConsumption(plan, false);
                total_demand += memConsumption;
            }
        }
        return total_demand;
    }


    private double valid_QPI_Bandwidth_demand(Map<Integer, Boolean> validationMap, SchedulingPlan plan, int socket_src, int socket_dst) {

        double total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {
            for (TopologyComponent parent : executor.getParents_keySet()) {
                for (ExecutionNode pE : parent.getExecutorList()) {
                    if (p(plan, pE, socket_src) && p(plan, executor, socket_dst)
                            && validationMap.get(executor.getExecutorID()) && validationMap.get(pE.getExecutorID())) {//DR
                        total_demand += executor.getTransConsumption(pE, plan, true);
                    }
                }
            }
        }
        return total_demand;
    }

    private double Communication_cycles_demand(SchedulingPlan plan, int socket_i, int socket_j) {

        double total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {
            for (TopologyComponent parent : executor.getParents_keySet()) {
                for (ExecutionNode pE : parent.getExecutorList()) {
                    if (p(plan, pE, socket_i) && p(plan, executor, socket_j)) {//DR
                        double executorCommConsumption = executor.getCommConsumption(pE, plan, false);
                        total_demand += executorCommConsumption;
                    }
                }
            }
        }
        return total_demand;
    }

    private double QPI_Bandwidth_demand(SchedulingPlan plan, int socket_i, int socket_j) {

        double total_demand = 0;
        plan.cache_clean();
        for (ExecutionNode executor : plan.graph.getExecutionNodeArrayList()) {
            for (TopologyComponent parent : executor.getParents_keySet()) {
                for (ExecutionNode pE : parent.getExecutorList()) {
                    if (p(plan, pE, socket_i) && p(plan, executor, socket_j)) {//DR
                        double executorTransConsumption = executor.getTransConsumption(pE, plan, false);
//                         LOG.info("BasicBoltBatchExecutor:" + executor.getExecutorID() + "\ttakes:" + executorTransConsumption);
                        total_demand += executorTransConsumption;
                    }
                }
            }
        }
        return total_demand;
    }

    /**
     * @param plan
     * @param k
     * @param socket
     * @return
     */
    private boolean p(SchedulingPlan plan, ExecutionNode k, int socket) {
        return plan.Allocated(k) && plan.allocation_decision(k) != -1 && plan.allocation_decision(k) == socket;
    }

    public boolean identical_Nodes(Map<Integer, Boolean> validationMap, SchedulingPlan plan, int a, int b) {


        if (valid_CPU_demand(validationMap, plan, a) != valid_CPU_demand(validationMap, plan, b)) {
            return false;
        }
        if (valid_Bandwidth_demand(validationMap, plan, a) != valid_Bandwidth_demand(validationMap, plan, b)) {
            return false;
        }

        //identical input traffic
        for (int i = 0; i < sockets; i++) {
            if (valid_QPI_Bandwidth_demand(validationMap, plan, a, i) != valid_QPI_Bandwidth_demand(validationMap, plan, b, i)) {
                return false;
            }
        }

        //identical output traffic
        for (int i = 0; i < sockets; i++) {
            if (valid_QPI_Bandwidth_demand(validationMap, plan, i, a) != valid_QPI_Bandwidth_demand(validationMap, plan, i, b)) {
                return false;
            }
        }

        return true;
//		//identical available cores
//		return valid_cores_demand(validationMap, plan, a) == valid_cores_demand(validationMap, plan, b);
    }


    public String show(SchedulingPlan plan) {

        StringBuilder sb = new StringBuilder();

        //CPU, memory and cores per socket
        for (int i = 0; i < sockets; i++) {
            double cpu_demand = CPU_demand(plan, i);
            double available_cpu = Available_CPU(i);
            sb.append("\noverhead_total demand: ").append(cpu_demand).append("\t available:").append(available_cpu).append("\t\tidle CPU resource on socket:\t").append(i).append("\tis:\t").append(String.format("%.2f", 100 * (available_cpu - cpu_demand)
                    / available_cpu)).append("%").append("\n");

            double bandwidth_demand = Bandwidth_demand(plan, i);
            double available_bandwidth = Available_bandwidth(i);
            sb.append("overhead_total demand: ").append(bandwidth_demand).append("\t available:").append(available_bandwidth).append("\t\tidle Bandwidth resource on socket:\t").append(i).append("\tis:\t").append(String.format("%.2f", 100 * (available_bandwidth - bandwidth_demand)
                    / available_bandwidth)).append("%").append("\n");

            double cores_demand = cores_demand(plan, i);
            double available_cores = Available_CORES(i);
            sb.append("\noverhead_total demand: ").append(cores_demand).append("\t available:").append(available_cpu).append("\t\tidle cores on socket:\t").append(i).append("\tis:\t").append(String.format("%.2f", 100 * (available_cores - cores_demand)
                    / available_cores)).append("%").append("\n");
        }


        //QPI
        for (int i = 0; i < sockets; i++) {
            for (int j = 0; j < sockets; j++) {
                if (i == j) {
                    continue;
                }
                double qpi_bandwidth_demand = QPI_Bandwidth_demand(plan, i, j);
                double available_qpi_bandwidth = Available_QPI_bandwidth(i, j);
                sb.append("overhead_total demand: ").append(qpi_bandwidth_demand).append("\t\tidle QPI bandwidth resource from socket:\t").append(i).append("\tto socket:").append(j).append("\tis:\t").append(String.format("%.2f", 100 * (available_qpi_bandwidth - qpi_bandwidth_demand)
                        / available_qpi_bandwidth)).append("%").append("\n");
            }
        }

        //CRMA cost
        for (int i = 0; i < sockets; i++) {
            for (int j = 0; j < sockets; j++) {

                double communication_cycles_demand = Communication_cycles_demand(plan, i, j);
                sb.append("communication_cycles_demand: ").append(communication_cycles_demand).append("\t\t from socket:\t").append(i).append("\tto socket:").append(j).append("\n");
            }
        }

        return sb.toString();
    }


    public void relax_reset() {
        this.relax_cpu = relax;
        this.relax_qpi = relax;
        this.relax_memory = relax;
        this.relax_cores = 0;
    }

    public void restore() {
        this.relax_cores = this.bk_relax_cores;
        this.relax_cpu = this.bk_relax_cpu;
        this.relax_qpi = this.bk_relax_qpi;
        this.relax_memory = this.bk_relax_memory;
    }

    public void backup() {
        this.bk_relax_cores = this.relax_cores;
        this.bk_relax_cpu = this.relax_cpu;
        this.bk_relax_qpi = this.relax_qpi;
        this.bk_relax_memory = this.relax_memory;
    }

    /**
     * 1
     * 2
     * 4
     * 8
     *
     * @param satisfy
     */
    public boolean adjust_satisfy(int satisfy) {
        boolean flag = false;
        switch (satisfy) {
            case 0: {//none satisfied
                this.relax_cpu += 0.1;
                this.relax_memory += 0.1;
                this.relax_qpi += 0.1;
                this.relax_cores += 1;
                break;
            }
            case 1: {//only CPU satisfied
                this.relax_memory += 0.1;
                this.relax_qpi += 0.1;
                this.relax_cores += 1;
                break;
            }
            case 2: {//only memory satisfied
                this.relax_cpu += 0.1;
                this.relax_qpi += 0.1;
                this.relax_cores += 1;
                break;
            }
            case 4: {//only qpi satisfied
                this.relax_memory += 0.1;
                this.relax_cpu += 0.1;
                this.relax_cores += 1;
                break;
            }
            case 8: {//only cores satisfied
                this.relax_cpu += 0.1;
                this.relax_memory += 0.1;
                this.relax_qpi += 0.1;
            }
            case 3: {//cpu and memory satisfied
                this.relax_qpi += 0.1;
                this.relax_cores += 1;
                break;
            }
            case 5: {//cpu and qpi satisfied
                this.relax_memory += 0.1;
                this.relax_cores += 1;
                break;
            }
            case 6: {//memory and qpi satisfied
                this.relax_cpu += 0.1;
                this.relax_cores += 1;
                break;
            }
            case 7: {//all except cores
                this.relax_cores += 1;
                break;
            }
            case 9: {//cpu and cores satisfied
                this.relax_qpi += 0.1;
                this.relax_memory += 0.1;
                break;
            }
            case 10: {//memory and cores satisfied
                this.relax_cpu += 0.1;
                this.relax_qpi += 0.1;
                break;
            }
            case 11: {//all except qpi
                this.relax_qpi += 0.1;
                break;
            }
            case 12: {//qpi and cores satisfied
                this.relax_cpu += 0.1;
                this.relax_memory += 0.1;
                break;
            }
            case 13: {//all except memory
                this.relax_memory += 0.1;
                break;
            }
            case 14: {//all except cpu
                this.relax_cpu += 0.1;
                break;
            }
        }

//        if (this.relax_cpu > 1 || this.relax_memory > 1.5 || this.relax_qpi > 1.5) {
//            SOURCE_RATE = SOURCE_RATE * 0.9;
//            flag = true;
//        }

//        if (this.relax_cpu > 2) {
//            this.relax_cpu = 2;
//        }
//        if (this.relax_memory > 2) {
//            this.relax_memory = 2;
//        }
//        if (this.relax_qpi > 2) {
//            this.relax_qpi = 2;
//        }

        setRelax_cpu(this.relax_cpu);
        setRelax_memory(this.relax_memory);
        setRelax_qpi(this.relax_qpi);
        setRelax_cores(this.relax_cores);
        return flag;
    }


    public boolean ResourceEfficient(SchedulingPlan new_plan, SchedulingPlan currentPlan) {
        double cpu_demand_new = 0;
        double cpu_demand_old = 0;
        double bandwidth_demand_new = 0;
        double bandwidth_demand_old = 0;

        for (int i = 0; i < sockets; i++) {
            cpu_demand_new += CPU_demand(new_plan, i);
            cpu_demand_old += CPU_demand(currentPlan, i);
            bandwidth_demand_new += Bandwidth_demand(new_plan, i);
            bandwidth_demand_old += Bandwidth_demand(currentPlan, i);
        }


        double qpi_bandwidth_demand_new = 0;
        double qpi_bandwidth_demand_old = 0;
        for (int i = 0; i < sockets; i++) {
            for (int j = 0; j < sockets; j++) {
                if (i == j) {
                    continue;
                }
                qpi_bandwidth_demand_new += QPI_Bandwidth_demand(new_plan, i, j);
                qpi_bandwidth_demand_new += QPI_Bandwidth_demand(currentPlan, i, j);
            }
        }
        return cpu_demand_old > cpu_demand_new && bandwidth_demand_old > bandwidth_demand_new && qpi_bandwidth_demand_old > qpi_bandwidth_demand_new;
    }

    public String relax_display() {
        return "\n================Constraints relax:==================\n " +
                "CPU relax of:" + relax_cpu + "\t, Memory relax of:" + relax_memory
                + "\t, QPI relax of:" + relax_qpi + "\t, cores relax: " + relax_cores;
    }

    public String constraintBy(int satisfy) {

        switch (satisfy) {
            case 0: {//none satisfied
                return "None satisfied";
            }
            case 1: {//only CPU satisfied
                return "only CPU satisfied";
            }
            case 2: {//only memory satisfied
                return "only memory satisfied";
            }
            case 4: {//only qpi satisfied
                return "only qpi satisfied";
            }
            case 8: {//only cores satisfied
                return "only cores satisfied";
            }
            case 3: {//cpu and memory satisfied
                return "cpu and memory satisfied";
            }
            case 5: {//cpu and qpi satisfied
                return "cpu and qpi satisfied";
            }
            case 6: {//memory and qpi satisfied
                return "memory and qpi satisfied";
            }
            case 7: {//all except cores
                return "all except cores";
            }
            case 9: {//cpu and cores satisfied
                return "cpu and cores satisfied";
            }
            case 10: {//memory and cores satisfied
                return "memory and cores satisfied";
            }
            case 11: {//all except qpi
                return "all except qpi";
            }
            case 12: {//qpi and cores satisfied
                return "qpi and cores satisfied";
            }
            case 13: {//all except memory
                return "all except memory";
            }
            case 14: {//all except cpu
                return "all except cpu";
            }
            case 15: {
                return "all satisfied";
            }
            default:
                return "undefined.";
        }
    }

}
