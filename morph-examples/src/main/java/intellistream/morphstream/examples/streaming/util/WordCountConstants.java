package intellistream.morphstream.examples.streaming.util;

import intellistream.morphstream.common.constants.BaseConstants;

public interface WordCountConstants extends BaseConstants {
    String PREFIX = "wc";
    int max_hz = 450000;

    interface Field extends BaseField {
        String WORD = "word";
        String COUNT = "count";
        String LargeData = "LD";
    }

    interface Conf extends BaseConf {
        String SPLITTER_THREADS = "wc.splitter.threads";
        String COUNTER_THREADS = "wc.counter.threads";
    }

    interface Component extends BaseComponent {
        String SPLITTER = "splitSentence";
        String COUNTER = "wordCount";
        String AGG = "aggregator";
    }

    interface TunedConfiguration {
        int Splitter_core1 = 1;
        int Counter_core1 = 1;
        int Splitter_core2 = 1;
        int Counter_core2 = 1;
        int Splitter_core4 = 1;
        int Counter_core4 = 1;
        int Splitter_core8 = 2;
        int Counter_core8 = 2;
        int Splitter_core16 = 1;
        int Counter_core16 = 1;
        int Splitter_core32 = 1;
        int Counter_core32 = 2;
        int Splitter_core8_HP = 1;
        int Counter_core8_HP = 4;
        int Splitter_core8_Batch2 = 1;
        int Counter_core8_Batch2 = 2;
        int acker_core8_Batch2 = 1;
        int Splitter_core8_Batch4 = 1;
        int Counter_core8_Batch4 = 16;
        int acker_core8_Batch4 = 1;
        int Splitter_core8_Batch8 = 8;
        int Counter_core8_Batch8 = 1;
        int acker_core8_Batch8 = 1;
        int Splitter_core32_HP_Batch = 8;
        int Counter_core32_HP_Batch = 2;
        int acker_core1 = 1;
        int acker_core2 = 1;
        int acker_core4 = 1;
        int acker_core8 = 1;
        int acker_core16 = 1;
        int acker_core32 = 2;
        int acker_core8_HP = 4;
        int acker_core32_HP_Batch = 1;
    }
}
