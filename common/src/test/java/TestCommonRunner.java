import common.collections.utils;
import org.junit.Test;
/**
 * Created by I309939 on 8/3/2016.
 */
public class TestCommonRunner {
    @Test
    public void TestDataGenerator() {
        utils test = new utils();
        String[] args = {
                "main",
                String.valueOf(1),//function
                String.valueOf(1),//Brisk.execution.runtime.tuple size per byte
                String.valueOf(2),//data skew factor
                "true",
                "false"};//whether print it out.
        utils.main(args);
    }
}
