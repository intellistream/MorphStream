/*
 * chombo: Hadoop Map Reduce utility
 * Author: Pranab Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package common.util.datatypes;
/**
 * Tabular data structure
 *
 * @author pranab
 */
public class TabularData {
    protected static final String DELIMETER = ",";
    protected int[][] table;
    protected int numRow;
    protected int numCol;
    protected String[] rowLabels;
    protected String[] colLabels;
    public TabularData() {
    }
    /**
     * @param numRow
     * @param numCol
     */
    public TabularData(int numRow, int numCol) {
        initialize(numRow, numCol);
    }
    /**
     * @param rowLabels
     * @param colLabels
     */
    public TabularData(String[] rowLabels, String[] colLabels) {
        initialize(rowLabels.length, colLabels.length);
        setLabels(rowLabels, colLabels);
    }
    /**
     * @param numRow
     * @param numCol
     */
    public void initialize(int numRow, int numCol) {
        table = new int[numRow][numCol];
        for (int r = 0; r < numRow; ++r) {
            for (int c = 0; c < numCol; ++c) {
                table[r][c] = 0;
            }
        }
        this.numRow = numRow;
        this.numCol = numCol;
    }
    /**
     * @param rowLabels
     * @param colLabels
     */
    public void setLabels(String[] rowLabels, String[] colLabels) {
        this.rowLabels = rowLabels;
        this.colLabels = colLabels;
    }
    /**
     * @param row
     * @param col
     * @param val
     */
    public void set(int row, int col, int val) {
        table[row][col] = val;
    }
    /**
     * @param row
     * @param col
     * @return
     */
    public int get(int row, int col) {
        return table[row][col];
    }
    /**
     * @param row
     * @return
     */
    public int[] getRow(int row) {
        return table[row];
    }
    /**
     * @param row
     * @param col
     * @param val
     */
    public void add(int row, int col, int val) {
        table[row][col] += val;
    }
    /**
     * add value to cell
     *
     * @param rowLabel
     * @param colLabel
     * @param val
     */
    public void add(String rowLabel, String colLabel, int val) {
        int[] rowCol = getRowCol(rowLabel, colLabel);
        table[rowCol[0]][rowCol[1]] += val;
    }
    /**
     * increments cell
     *
     * @param row
     * @param col
     */
    public void increment(int row, int col) {
        table[row][col] += 1;
    }
    /**
     * increments cell
     *
     * @param rowLabel
     * @param colLabel
     */
    public void increment(String rowLabel, String colLabel) {
        int[] rowCol = getRowCol(rowLabel, colLabel);
        table[rowCol[0]][rowCol[1]] += 1;
    }
    /**
     * sum of SchemaRecord
     *
     * @param row
     * @return
     */
    public int getRowSum(int row) {
        int sum = 0;
        for (int c = 0; c < numCol; ++c) {
            sum += table[row][c];
        }
        return sum;
    }
    /**
     * sum of column
     *
     * @param col
     * @return
     */
    public int getColumnSum(int col) {
        int sum = 0;
        for (int r = 0; r < numRow; ++r) {
            sum += table[r][col];
        }
        return sum;
    }
    /**
     * serializes table
     *
     * @return
     */
    public String serialize() {
        StringBuilder stBld = new StringBuilder();
        for (int r = 0; r < numRow; ++r) {
            for (int c = 0; c < numCol; ++c) {
                stBld.append(table[r][c]).append(DELIMETER);
            }
        }
        return stBld.substring(0, stBld.length() - 1);
    }
    /**
     * serialize SchemaRecord
     *
     * @param row
     * @return
     */
    public String serializeRow(int row) {
        StringBuilder stBld = new StringBuilder();
        for (int c = 0; c < numCol; ++c) {
            stBld.append(table[row][c]).append(DELIMETER);
        }
        return stBld.substring(0, stBld.length() - 1);
    }
    /**
     * deserialize table
     *
     * @param data
     */
    public void deseralize(String data) {
        String[] items = data.split(DELIMETER);
        int k = 0;
        for (int r = 0; r < numRow; ++r) {
            for (int c = 0; c < numCol; ++c) {
                table[r][c] = Integer.parseInt(items[k++]);
            }
        }
    }
    /**
     * deserialize SchemaRecord
     *
     * @param data
     * @param row
     */
    public void deseralizeRow(String data, int row) {
        String[] items = data.split(DELIMETER);
        int k = 0;
        for (int c = 0; c < numCol; ++c) {
            table[row][c] = Integer.parseInt(items[k++]);
        }
    }
    /**
     * SchemaRecord and column index
     *
     * @param rowLabel
     * @param colLabel
     * @return
     */
    private int[] getRowCol(String rowLabel, String colLabel) {
        int[] rowCol = new int[2];
        rowCol[0] = rowCol[1] = -1;
        int i = 0;
        for (String label : rowLabels) {
            if (label.equals(rowLabel)) {
                rowCol[0] = i;
                break;
            }
            ++i;
        }
        i = 0;
        for (String label : colLabels) {
            if (label.equals(colLabel)) {
                rowCol[1] = i;
                break;
            }
            ++i;
        }
        return rowCol;
    }
}