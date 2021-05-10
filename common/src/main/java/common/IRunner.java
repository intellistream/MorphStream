package common;
import com.beust.jcommander.Parameter;
import common.collections.OsUtils;
public interface IRunner {
    String RUN_LOCAL = "local";
    String RUN_REMOTE = "remote";

    int CCOption_LOCK = 0;
    int CCOption_OrderLOCK = 1;
    int CCOption_LWM = 2;
    int CCOption_TStream = 3;
    int CCOption_SStore = 4;
}
