package common.constants;
public interface OnlineBidingSystemConstants extends BaseConstants {
    String PREFIX = "ob";
    interface Field {
        String TIME = "time";
        String TEXT = "text";
        String STATE = "state";
        String FLAG = "flag";
    }
    interface Conf extends BaseConf {
        String OB_THREADS = "ob.transaction.threads";
    }
    interface Component extends BaseComponent {
        String OB = "online_biding";
    }
    interface Constant {
        int NUM_ACCESSES_PER_BUY = 1;// each time only bid one item..
        int NUM_ACCESSES_PER_TOP = 20;// each time top up 20 items.
        int NUM_ACCESSES_PER_ALERT = 20;// each time alert 20 items.
        long MAX_BUY_Transfer = 20;
        long MAX_TOP_UP = 20;
        int MAX_Price = 100;
    }
}
