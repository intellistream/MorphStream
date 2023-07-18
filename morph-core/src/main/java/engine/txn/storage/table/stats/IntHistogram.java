package engine.txn.storage.table.stats;

import engine.txn.operator.QueryPlan;
import engine.txn.storage.datatype.DataBox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * A class that represents the histogram of an integer column.
 * An instance of IntHistogram exposes statistics helpful
 * in the calculation of a reduction factor with rangeMin,
 * rangeMax, and numDistinct.
 */
public class IntHistogram implements Histogram<Integer> {
    private static final int NUM_BUCKETS = 10;
    private static final int DEFAULT_BUCKET_SIZE = 10;
    private final boolean estimate;
    private int rangeMin;
    private int rangeMax;
    private int numDistinct;
    private List<Bucket<Integer>> buckets;
    private HashSet<Integer> entrySet;

    /**
     * Creates a new IntHistogram with no entries.
     */
    public IntHistogram() {
        this.estimate = false;
        this.rangeMin = NUM_BUCKETS / 2 * DEFAULT_BUCKET_SIZE * -1;
        this.rangeMax = NUM_BUCKETS / 2 * DEFAULT_BUCKET_SIZE;
        this.buckets = new ArrayList<>();
        this.entrySet = new HashSet<>();
        // By default create a histogram from -50 to 50 with bucket size 10.
        for (int i = 0; i < NUM_BUCKETS; i++) {
            int start = this.rangeMin + (i * DEFAULT_BUCKET_SIZE);
            int end = start + DEFAULT_BUCKET_SIZE;
            this.buckets.add(new Bucket<>(start, end));
        }
    }

    /**
     * Creates a new IntHistogram with given buckets and
     * an estimate number of distinct entries.
     *
     * @param buckets     the buckets with counts from a previous IntHistogram
     * @param numDistinct the estimate number of distinct entries for this
     */
    public IntHistogram(List<Bucket<Integer>> buckets, int numDistinct) {
        this.estimate = true;
        this.numDistinct = numDistinct;
        this.buckets = buckets;
    }

    /**
     * Creates a new IntHistogram that would result from
     * applying the given reduction factor over this.
     *
     * @param reductionFactor the reduction factor to use to create a new IntHistogram
     * @return a new IntHistogram based off of this and params
     */
    public IntHistogram copyWithReduction(float reductionFactor) {
        List<Bucket<Integer>> copyBuckets = new ArrayList<>();
        for (Bucket<Integer> bucket : buckets) {
            int bucketCount = bucket.getCount();
            int bucketStart = bucket.getStart();
            int bucketEnd = bucket.getEnd();
            Bucket<Integer> copyBucket = new Bucket<>(bucketStart, bucketEnd);
            copyBucket.increment((int) (bucketCount * reductionFactor));
            copyBuckets.add(copyBucket);
        }
        int copyNumDistinct = (int) Math.ceil(this.getNumDistinct() * reductionFactor);
        return new IntHistogram(copyBuckets, copyNumDistinct);
    }

    /**
     * Creates a new IntHistogram that would result from
     * applying the given predicate and value_list over this.
     *
     * @param predicate the predicate to use to create a new IntHistogram
     * @param value     the value_list to compare against
     * @return a new IntHistogram based off of this and params
     */
    public IntHistogram copyWithPredicate(QueryPlan.PredicateOperator predicate,
                                          DataBox value) {
        List<Bucket<Integer>> copyBuckets = new ArrayList<>();
        for (Bucket<Integer> bucket : buckets) {
            int bucketCount = bucket.getCount();
            int bucketStart = bucket.getStart();
            int bucketEnd = bucket.getEnd();
            int bucketSize = bucketEnd - bucketStart;
            int predValue = value.getInt();
            Bucket<Integer> copyBucket = new Bucket<>(bucketStart, bucketEnd);
            switch (predicate) {
                case EQUALS:
                    if (predValue >= bucketStart && predValue < bucketEnd) {
                        copyBucket.increment(bucketCount / bucketSize);
                    }
                    break;
                case LESS_THAN:
                    if (predValue > bucketEnd) {
                        copyBucket.increment(bucketCount);
                    } else if (predValue >= bucketStart) {
                        copyBucket.increment(bucketCount * (predValue - bucketStart) / bucketSize);
                    }
                    break;
                case LESS_THAN_EQUALS:
                    if (predValue > bucketEnd) {
                        copyBucket.increment(bucketCount);
                    } else if (predValue >= bucketStart) {
                        copyBucket.increment(bucketCount * (predValue - bucketStart) / bucketSize);
                        copyBucket.increment(bucketCount / bucketSize);
                    }
                    break;
                case GREATER_THAN:
                    if (predValue < bucketStart) {
                        copyBucket.increment(bucketCount);
                    } else if (predValue < bucketEnd) {
                        copyBucket.increment(bucketCount * (bucketEnd - predValue) / bucketSize);
                    }
                    break;
                case GREATER_THAN_EQUALS:
                    if (predValue <= bucketStart) {
                        copyBucket.increment(bucketCount);
                    } else if (predValue < bucketEnd) {
                        copyBucket.increment(bucketCount * (bucketEnd - predValue) / bucketSize);
                    }
                default:
                    break;
            }
            copyBuckets.add(copyBucket);
        }
        float reductionFactor = this.computeReductionFactor(predicate, value);
        int copyNumDistinct = (int) (this.getNumDistinct() * reductionFactor);
        return new IntHistogram(copyBuckets, copyNumDistinct);
    }

    @Override
    public int getSampleCount() {
        return 0;
    }

    /**
     * Gets the buckets of this IntHistogram.
     *
     * @return this IntHistogram's buckets
     */
    public List<Bucket<Integer>> getAllBuckets() {
        return this.buckets;
    }

    @Override
    public Long get(Integer value) {
        return null;
    }

    @Override
    public Collection<Integer> values() {
        return null;
    }

    /**
     * Adds an entry into this IntHistogram.
     * Buckets are start inclusive and end exclusive.
     */
    public void add(Integer value) {
        if (value >= this.rangeMax || value < this.rangeMin) {
            this.refactorBuckets(value);
        }
        //increment the corresponding bucket.
        for (Bucket<Integer> bucket : this.buckets) {
            if (value >= bucket.getStart() && value < bucket.getEnd()) {
                bucket.increment();
            }
        }
        this.entrySet.add(value);
    }

    /**
     * Removes an entry from this IntHistogram.
     */
    public void removeValue(Integer value) {
        for (Bucket<Integer> bucket : this.buckets) {
            if (value >= bucket.getStart() && value < bucket.getEnd()) {
                bucket.decrement();
            }
        }
    }

    /**
     * Computes the reduction factor that a predicate and value_list result in
     * over this IntHistogram. You'll find instance methods of this class
     * helpful - note that many are estimates and not exact. For the range
     * of values calculation, please use (max - min) as opposed to
     * (max - min + 1) - the buckets backing this histogram are end exclusive.
     *
     * @param predicate the predicate to compute reduction factor for
     * @param value     the value_list to compare against
     * @return computed reduction factor
     */
    public float computeReductionFactor(QueryPlan.PredicateOperator predicate,
                                        DataBox value) {
        /* TODO: Implement me! */
        float rf;
        switch (predicate) {
            case EQUALS:
                rf = (float) 1 / (float) getNumDistinct();
                break;
            case LESS_THAN:
                rf = (float) (getMaxValueIndex() - value.getInt()) / (float) (getMaxValueIndex() - getMinValueIndex());
                break;
            case LESS_THAN_EQUALS:
                rf = (float) (getMaxValueIndex() - value.getInt() + 1) / (float) (getMaxValueIndex() - getMinValueIndex());
                break;
            case GREATER_THAN:
                rf = (float) (getMaxValueIndex() - value.getInt()) / (float) (getMaxValueIndex() - getMinValueIndex());
                break;
            case GREATER_THAN_EQUALS:
                rf = (float) (getMaxValueIndex() - value.getInt() + 1) / (float) (getMaxValueIndex() - getMinValueIndex());
                break;
            default:
                rf = (float) 1 / (float) 10;
        }
        return rf;
    }

    /**
     * Gets the number of entries within a certain range.
     *
     * @param start the start value_list of the range in question
     * @param end   the end value_list of the range in question
     * @return number of entries in range
     */
    public int getEntriesInRange(Integer start, Integer end) {
        int entries = 0;
        for (Bucket<Integer> bucket : this.buckets) {
            int bucketStart = bucket.getStart();
            int bucketEnd = bucket.getEnd();
            int bucketSize = bucketEnd - bucketStart;
            if (bucketStart >= end) {
                break;
            }
            // Note that the number returned is an approximation.
            if (bucketStart >= start && bucketEnd <= end) {
                entries += bucket.getCount();
            } else if (bucketStart >= start && bucketStart <= end) {
                entries += (int) Math.ceil(bucket.getCount() * ((end - bucketStart) / (float) bucketSize));
            } else if (bucketEnd <= end && bucketEnd >= start) {
                entries += (int) Math.ceil(bucket.getCount() * ((bucketEnd - start) / (float) bucketSize));
            }
        }
        return entries;
    }

    /**
     * Gets the estimate minimum value_list of this. The estimate minimum
     * value_list is the start value_list of the leftmost bucket with some entries.
     *
     * @return estimate minimum value_list
     */
    public int getMinValueIndex() {
        int minValue = this.rangeMin;
        for (Bucket<Integer> bucket : this.buckets) {
            if (bucket.getCount() > 0) {
                minValue = bucket.getStart();
                break;
            }
        }
        return minValue;
    }

    /**
     * Gets the estimate maximum value_list of this. The estimate maximum
     * value_list is the end value_list of the rightmost bucket with some entries.
     *
     * @return estimate maximum value_list
     */
    public int getMaxValueIndex() {
        int maxValue = this.rangeMax;
        for (int i = this.buckets.size() - 1; i >= 0; i--) {
            Bucket<Integer> bucket = this.buckets.get(i);
            if (bucket.getCount() > 0) {
                maxValue = bucket.getEnd();
                break;
            }
        }
        return maxValue;
    }

    @Override
    public Integer getMinValue() {
        return null;
    }

    @Override
    public Integer getMaxValue() {
        return null;
    }

    /**
     * Gets the estimate number of distinct entries in this histogram.
     *
     * @return estimate number of distinct entries
     */
    public int getNumDistinct() {
        if (this.estimate) {
            return this.numDistinct;
        } else {
            return entrySet.size();
        }
    }

    /**
     * Refactors the buckets backing this IntHistogram to allow
     * the given value_list to belong in a bucket of this IntHistogram.
     *
     * @param value the value_list to refactor the buckets with
     */
    private void refactorBuckets(int value) {
        while (value < this.rangeMin || value >= this.rangeMax) {
            int newRangeMin = this.rangeMin * 2;
            int newRangeMax = this.rangeMax * 2;
            int newRange = newRangeMax - newRangeMin;
            int newBucketSize = newRange / NUM_BUCKETS;
            List<Bucket<Integer>> newBuckets = new ArrayList<>();
            for (int i = 0; i < NUM_BUCKETS; i++) {
                int newStart = newRangeMin + i * newBucketSize;
                int newEnd = newStart + newBucketSize;
                Bucket<Integer> newBucket = new Bucket<>(newStart, newEnd);
                for (int j = 0; j < NUM_BUCKETS; j++) {
                    Bucket<Integer> oldBucket = this.buckets.get(j);
                    int oldStart = oldBucket.getStart();
                    int oldEnd = oldBucket.getEnd();
                    if (newStart <= oldStart && oldEnd <= newEnd) {
                        newBucket.increment(oldBucket.getCount());
                    }
                }
                newBuckets.add(newBucket);
            }
            this.rangeMin = newRangeMin;
            this.rangeMax = newRangeMax;
            this.buckets = newBuckets;
        }
    }
}