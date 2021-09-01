package common;

public interface CONTROL {
    boolean enable_log = false;
    boolean enable_debug = false;//some critical debug section.
    int kMaxThreadNum = 40;
    int MeasureStart = 0;//10_000;//server needs at least 10,000 to compile, so skip them.
    int MeasureBound = 1_000;

    //common.combo optimization
    boolean enable_app_combo = true;//compose all operators into one.
    int combo_bid_size = 1;//reduce conflict. NOT applicable to LAL, LWM and PAT (must set to one).
    int sink_combo_bid_size = 200;//reduce conflict. NOT applicable to LAL, LWM and PAT (must set to one).
    //db related.
    boolean enable_shared_state = true;//this is for transactional state mgmt.
    boolean enable_states_partition = true;//must be enabled for PAT/SSTORE.
    boolean enable_TSTREAM = true;
    //pre- and post -compute
    boolean enable_pre_compute = false;//not in use.
    boolean enable_post_compute = true;
    //latency related.
    boolean enable_latency_measurement = false;//
    //    boolean enable_admission_control = enable_latency_measurement;//only enable for TStream
    //profile related.
    boolean enable_profile = true;//enable this only when we want to test for breakdown.
    //engine related.
    boolean enable_engine = true;//1. enable TP_engine. Always enabled. There's no meaning if we disable engine for T-Stream.
    boolean enable_numa_placement = true;//thread placement. always on.
    //used for NUMA-aware partition engine
    boolean enable_work_partition = false; // 2. this is a sub-option, only useful when engine is enabled.
    int island = -1;//-1 stands for one engine per core; -2 stands for one engine per socket.
    int CORE_PER_SOCKET = 2;//configure this for NUMA placement please.
    int NUM_OF_SOCKETS = 1;//configure this for NUMA placement please.
    //single engine with work-stealing.
    boolean enable_work_stealing = true; // won't affect is island=-1 under partition.
    boolean enable_mvcc = false;// mvcc is only required in StreamLedger for cross-dependency reading.
    boolean enable_speculative = false;//work in future!
//    boolean enable_pushdown = false;//enabled by default.
}