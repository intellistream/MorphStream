package sesame.optimization;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.routing.RoutingPlan;
/**
 * Created by I309939 on 11/8/2016.
 */
public class ExecutionPlan {
    final SchedulingPlan SP;
    private final RoutingPlan RP;
    /**
     * TODO: In the future release, the set_executor_ready profiling should be automatically done by the system, but now it is done by hand...
     * set profile flag to be true, and we fix a plan for the graph that we are going to profile with.
     * to make sure smooth transition from such hand-coded profiling to automatically profiling,
     * I let the plan being build based on a intermediate file, which can be produced by hand-coding or by program.
     */
    public boolean profile = false;
    /**
     * switch on and off benchmark.
     */
    public boolean benchmark = false;
    public int profile_executor = -1;//start from -1;
    public ExecutionPlan() {
        SP = null;
        RP = null;
    }
    public ExecutionPlan(SchedulingPlan SP, RoutingPlan RP) {
        this.RP = RP;
        this.SP = SP;
    }
    public SchedulingPlan getSP() {
        return SP;
    }
    public boolean isProfile(int executorID) {
        return profile && (profile_executor == executorID);
    }
    public void setProfile() {
        this.profile = true;
    }
    public void disable_profile() {
        this.profile = false;
    }
    /**
     * correct one.
     *
     * @param executorID
     * @return
     */
    public int toSocket(int executorID) {
        if (SP == null) return 0;//default is 0
        return SP.allocation_decision(executorID);
    }
//	/**
//	 * Ad-hoc test purpose.
//	 *
//	 * @param executorID
//	 * @return
//	 */
//	public int toSocket(int executorID) {
//		if (executorID == 0 ) {//
//			return 0;
//		} else {
//			return 0;
//		}
//	}
}
