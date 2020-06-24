package application.helper.wrapper;

import application.helper.wrapper.basic.StateWrapper;

import java.util.Random;

/**
 * create x (e.g., 20000) different values in-advance.
 * The summation of those 20k numbers have a uniform distribution over 0 to 99.
 */
public class ValueStatesWrapper extends StateWrapper {

    public static final String split_expression = ",";
    private static final long serialVersionUID = 5362340617763107581L;
    private int N = 20000;
    private String[] values = new String[N];
    private int count = 0;

    public ValueStatesWrapper(int tuple_size) {
        super(0);
        Random r = new Random();
        for (int i = 0; i < N; i++) {
            double sum = r.nextInt(10);
            StringBuilder sb = new StringBuilder();
            //how many double numbers in this value list, that has a value summation of ``sum".
            for (int j = 0; j < tuple_size; j++) {
                double item = r.nextDouble();
                sum -= item;
                if (sum < 0) {
                    sum = 0;
                    item = 0.0;
                }
                String s = String.format("%.2f", sum);
                //long size_of_eachElement = MemoryUtil.deepMemoryUsageOf(s);
                sb.append(s);
                sb.append(split_expression);
            }
            values[i] = sb.toString();
        }
    }


    /**
     * Because String is primitive data structure, it returns by value? No, it is still a pointer reference to char array.
     *
     * @return a new copy of states.
     */
    @Override
    public String getTuple_states() {
        return values[count++ % N];
    }
}