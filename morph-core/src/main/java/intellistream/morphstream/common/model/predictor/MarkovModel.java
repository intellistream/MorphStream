package intellistream.morphstream.common.model.predictor;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * @author maycon
 */
public class MarkovModel {
    private List<String> states;
    private double[][] stateTransitionProb;
    private int numStates;

    public MarkovModel(String model) {
        Scanner scanner = new Scanner(model);
        int lineCount = 0;
        int row = 0;
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (0 == lineCount) {
                //states
                String[] items = line.split(",");
                states = Arrays.asList(items);
                numStates = items.length;
                stateTransitionProb = new double[numStates][numStates];
            } else {
                //populate state transtion probability
                deseralizeTableRow(stateTransitionProb, line, ",", row, numStates);
                ++row;
            }
            ++lineCount;
        }
        scanner.close();
    }

    /**
     * @param table
     * @param data
     * @param delim
     * @param row
     * @param numCol
     */
    private void deseralizeTableRow(double[][] table, String data, String delim, int row, int numCol) {
        String[] items = data.split(delim);
        if (items.length != numCol) {
            throw new IllegalArgumentException("SchemaRecord serialization failed, number of tokens in string does not match with number of columns");
        }
        for (int c = 0; c < numCol; ++c) {
            table[row][c] = Double.parseDouble(items[c]);
        }
    }

    public List<String> getStates() {
        return states;
    }

    public double[][] getStateTransitionProb() {
        return stateTransitionProb;
    }

    public int getNumStates() {
        return numStates;
    }
}
