package application.constants;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface VoIPSTREAMConstants extends BaseConstants {
    String PREFIX = "vs";
    int max_hz = 120000;

    interface Conf extends BaseConf {
        String GENERATOR_POPULATION = "vs.generator.population";
        String GENERATOR_ERROR_PROB = "vs.generator.error_prob";

        String FILTER_NUM_ELEMENTS = "vs.%s.num_elements";
        String FILTER_BUCKETS_PEL = "vs.%s.buckets_per_element";
        String FILTER_BUCKETS_PWR = "vs.%s.buckets_per_word";
        String FILTER_BETA = "vs.%s.beta";

        String SCORE_THRESHOLD_MIN = "vs.%s.threshold.min";
        String SCORE_THRESHOLD_MAX = "vs.%s.threshold.max";

        String ACD_DECAY_FACTOR = "vs.acd.decay_factor";

        String FOFIR_WEIGHT = "vs.fofir.weight";
        String URL_WEIGHT = "vs.url.weight";
        String ACD_WEIGHT = "vs.acd.weight";

        String VAR_DETECT_APROX_SIZE = "vs.variation.aprox_size";
        String VAR_DETECT_ERROR_RATE = "vs.variation.error_rate";

        String VAR_DETECT_THREADS = "vs.vardetect.threads";
        String VAR_DETECT_Split_THREADS = "vs.vardetectsplit.threads";
        String ECR_THREADS = "vs.ecr.threads";
        String RCR_THREADS = "vs.rcr.threads";
        String ENCR_THREADS = "vs.encr.threads";
        String ECR24_THREADS = "vs.ecr24.threads";
        String CT24_THREADS = "vs.ct24.threads";
        String FOFIR_THREADS = "vs.fofir.threads";
        String URL_THREADS = "vs.url.threads";
        String ACD_THREADS = "vs.acd.threads";
        String SCORER_THREADS = "vs.scorer.threads";
    }

    interface Component extends BaseComponent {
        String VARIATION_DETECTOR_Split = "VariationDetectorSplitBolt";
        String VOICE_DISPATCHER = "VoiceDispatcherBolt";
        String RCR = "RCRFilterBolt";
        String ECR = "ECRFilterBolt";
        String ENCR = "ENCRFilterBolt";
        String ECR24 = "ECR24FilterBolt";
        String CT24 = "CT24FilterBolt";
        String FOFIR = "FoFiRModuleBolt";
        String URL = "URLModuleBolt";
        String ACD = "ACDModuleBolt";
        String GLOBAL_ACD = "GlobalACDModuleBolt";
        String SCORER = "ScorerBolt";
    }

    interface Field extends BaseField {
        String VARIATION_DETECTOR_Split_Key = "VARIATION_DETECTOR_Split_Key";


        String CALLING_NUM = "callingNumber";
        String CALLED_NUM = "calledNumber";
        String TIMESTAMP = "timestamp";
        String SCORE = "score";
        String RECORD = "record";
        String AVERAGE = "average";
        String CALLTIME = "calltime";
        String NEW_CALLEE = "newCallee";
        String RATE = "rate";
        String RATE_KEY = "rate_KEY";
        String ANSWER_TIME = "answerTime";
    }

    interface Stream extends BaseStream {
        String BACKUP = "backupStream";
    }

    interface TunedConfiguration {

        int VAR_DETECT_THREADS_core1 = 1;
        int FOFIR_THREADS_core1 = 1;

        int VAR_DETECT_THREADS_core2 = 1;
        int FOFIR_THREADS_core2 = 1;

        int VAR_DETECT_THREADS_core4 = 1;
        int FOFIR_THREADS_core4 = 1;

        int VAR_DETECT_THREADS_core8 = 1;
        int FOFIR_THREADS_core8 = 8;

        int VAR_DETECT_THREADS_core16 = 1;
        int FOFIR_THREADS_core16 = 1;
        int VAR_DETECT_THREADS_core32 = 16;
        int FOFIR_THREADS_core32 = 16;
        int VAR_DETECT_THREADS_core8_HP = 4;
        int FOFIR_THREADS_core8_HP = 4;


        int VAR_DETECT_THREADS_core8_Batch2 = 1;
        int FOFIR_THREADS_core8_Batch2 = 2;
        int VAR_DETECT_THREADS_core8_Batch4 = 1;
        int FOFIR_THREADS_core8_Batch4 = 4;
        int VAR_DETECT_THREADS_core8_Batch8 = 1;
        int FOFIR_THREADS_core8_Batch8 = 4;

        int acker_core8_Batch2 = 1;
        int acker_core8_Batch4 = 1;
        int acker_core8_Batch8 = 4;


        int VAR_DETECT_THREADS_core32_HP_Batch = 1;
        int FOFIR_THREADS_core32_HP_Batch = 4;

        int acker_core1 = 1;
        int acker_core2 = 1;
        int acker_core4 = 1;
        int acker_core8 = 8;
        int acker_core16 = 1;
        int acker_core32 = 0;//disable ack
        int acker_core8_HP = 1;

        int acker_core32_HP_Batch = 1;

    }
}
