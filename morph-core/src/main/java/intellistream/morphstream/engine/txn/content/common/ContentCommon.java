package intellistream.morphstream.engine.txn.content.common;

public class ContentCommon {
    public static final int kRecycleLength = 100;
    //Note, this is a workaround for Java for lack of pre-compiler.
    //Alternative content type: LOCK_CONTENT,TO_CONTENT, LWM_CONTENT
    public static String content_type;//here, we configure which d_record should we use.
    public static int loggingRecord_type;//here, we configure which logging record should we use.
}
