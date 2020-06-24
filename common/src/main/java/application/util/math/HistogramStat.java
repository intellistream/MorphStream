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

package application.util.math;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author pranab
 */
public class HistogramStat {
    private int binWidth;
    private Map<Integer, Bin> binMap = new HashMap<>();
    private int count;
    private double sum = 0.0;

    /**
     * @param binWidth
     */
    public HistogramStat(int binWidth) {
        super();
        this.binWidth = binWidth;
    }

    /**
     * @param value
     */
    public void add(int value) {
        add(value, 1);
    }

    /**
     * @param value
     * @param count
     */
    public void add(int value, int count) {
        int index = value / binWidth;
        Bin bin = binMap.computeIfAbsent(index, Bin::new);
        bin.addCount(count);
        this.count += count;
        sum += value * count;
    }

    /**
     * @param confidenceLimitPercent
     * @return
     */
    public int[] getConfidenceBounds(int confidenceLimitPercent) {
        int[] confidenceBounds = new int[2];

        int mean = getMean();
        int meanIndex = mean / binWidth;
        int confCount = 0;
        int confidenceLimit = (count * confidenceLimitPercent) / 100;
        int binCount = 0;
        Bin bin = binMap.get(meanIndex);
        if (null != bin) {
            confCount += bin.getCount();
            ++binCount;
        }

        //starting for mean index extend to both sides to include other bins
        int offset = 1;
        for (; binCount < binMap.size(); ++offset) {
            bin = binMap.get(meanIndex + offset);
            if (null != bin) {
                confCount += bin.getCount();
                ++binCount;
            }
            bin = binMap.get(meanIndex - offset);
            if (bin != null) {
                confCount += bin.getCount();
                ++binCount;
            }
            if (confCount >= confidenceLimit) {
                break;
            }
        }

        double avBinWidth = binWidth > 1 ? 0.5 : 0.0;
        confidenceBounds[0] = (int) ((((double) (meanIndex - offset)) + avBinWidth) * binWidth);
        confidenceBounds[1] = (int) ((((double) (meanIndex + offset)) + avBinWidth) * binWidth);
        return confidenceBounds;
    }

    /**
     * @return
     */
    public int getMean() {
        int mean = (int) (sum / count);
        return mean;
    }

    /**
     * @return
     */
    public int getCount() {
        return count;
    }

    /**
     * @return
     */
    public HistogramStat.Bin[] getSortedBins() {
        Bin[] bins = new Bin[binMap.size()];
        int i = 0;
        for (Integer index : binMap.keySet()) {
            Bin bin = binMap.get(index);
            bins[i++] = bin;
        }
        Arrays.sort(bins);
        return bins;
    }

    /**
     * @author pranab
     */
    private static class Bin implements Comparable<Bin> {
        private int index;
        private int count;

        public Bin(int index) {
            super();
            this.index = index;
        }

        public Bin(int index, int count) {
            super();
            this.index = index;
            this.count = count;
        }

        public void addCount(int count) {
            this.count += count;
        }

        @Override
        public int compareTo(Bin that) {
            return this.index < that.index ? -1 : (this.index > that.index ? 1 : 0);
        }

        public int getIndex() {
            return index;
        }

        public int getCount() {
            return count;
        }
    }

}