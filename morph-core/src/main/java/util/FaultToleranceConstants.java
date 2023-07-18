package util;

public class FaultToleranceConstants {
    public static int FTOption_ISC = 1;
    public static int FTOption_WSC = 2;
    public static int FTOption_PATH = 3;
    public static int FTOption_LV = 4;
    public static int FTOption_Dependency = 5;
    public static int FTOption_Command = 6;
    public static int LOGOption_no = 0;
    public static int LOGOption_wal = 1;
    public static int LOGOption_path = 2;
    public static int LOGOption_lv = 3;
    public static int LOGOption_dependency = 4;
    public static int LOGOption_command = 5;
    public enum FaultToleranceStatus {
        NULL,Undo,Persist,Commit,Snapshot
    }
    public enum CompressionType {
        None, XOR, Delta2Delta, Delta, RLE, Dictionary, Snappy, Zigzag, Optimize
    }
}
