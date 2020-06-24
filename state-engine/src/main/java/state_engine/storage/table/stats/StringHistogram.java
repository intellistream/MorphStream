package state_engine.storage.table.stats;

import state_engine.query.QueryPlan.PredicateOperator;
import state_engine.storage.datatype.DataBox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class StringHistogram implements Histogram<String> {
    public static String alphaNumeric = "abcdefghijklmnopqrstuvwxyz0123456789";

    private boolean estimate;
    private int numDistinct;

    private List<Bucket<String>> buckets;
    private HashSet<String> entrySet;

    public StringHistogram() {
        this.estimate = false;

        this.buckets = new ArrayList<>();
        this.entrySet = new HashSet<>();

        for (int i = 0; i < alphaNumeric.length(); i++) {
            this.buckets.add(new Bucket<>(alphaNumeric.substring(i, i + 1)));
        }
    }

    public StringHistogram(List<Bucket<String>> buckets, int numDistinct) {
        this.estimate = true;
        this.numDistinct = numDistinct;

        this.buckets = buckets;
    }

    public StringHistogram copyWithReduction(float reductionFactor) {
        List<Bucket<String>> copyBuckets = new ArrayList<>();

        for (int i = 0; i < this.buckets.size(); i++) {
            Bucket<String> bucket = this.buckets.get(i);
            int bucketCount = bucket.getCount();

            Bucket<String> copyBucket = new Bucket<>(alphaNumeric.substring(i, i + 1));
            copyBucket.increment((int) (bucketCount * reductionFactor));
            copyBuckets.add(copyBucket);
        }

        int copyNumDistinct = (int) Math.ceil(this.getNumDistinct() * reductionFactor);
        return new StringHistogram(copyBuckets, copyNumDistinct);
    }

    public StringHistogram copyWithPredicate(PredicateOperator predicate,
                                             DataBox value) {
        List<Bucket<String>> copyBuckets = new ArrayList<>();

        for (int i = 0; i < this.buckets.size(); i++) {
            Bucket<String> bucket = this.buckets.get(i);
            int bucketCount = bucket.getCount();

            int predValue = alphaNumeric.indexOf(value.getString().charAt(0));
            int bucketValue = i;

            Bucket<String> copyBucket = new Bucket<>(alphaNumeric.substring(i, i + 1));

            switch (predicate) {
                case LESS_THAN:
                    if (predValue > bucketValue) {
                        copyBucket.increment(bucketCount);
                    }
                    break;
                case GREATER_THAN:
                    if (predValue < bucketValue) {
                        copyBucket.increment(bucketCount);
                    }
                    break;
                default:
                    break;
            }

            copyBuckets.add(copyBucket);
        }

        float reductionFactor = this.computeReductionFactor(predicate, value);
        int copyNumDistinct = (int) (this.getNumDistinct() * reductionFactor);
        return new StringHistogram(copyBuckets, copyNumDistinct);
    }

    @Override
    public int getSampleCount() {
        return 0;
    }

    public float computeReductionFactor(PredicateOperator predicate,
                                        DataBox value) {
        float reductionFactor = 1;

        int predValue = alphaNumeric.indexOf(value.getString().charAt(0));
        int minValue = this.getMinValueIndex();
        int maxValue = this.getMaxValueIndex();

        float numDistinct = this.getNumDistinct();
        float range = maxValue - minValue + 1;

        switch (predicate) {
            case LESS_THAN:
                reductionFactor = (predValue - minValue) / range;
                break;
            case GREATER_THAN:
                reductionFactor = (maxValue - predValue) / range;
                break;
            default:
                break;
        }

        return reductionFactor;
    }

    public List<Bucket<String>> getAllBuckets() {
        return this.buckets;
    }

    @Override
    public Long get(String value) {
        return null;
    }

    @Override
    public Collection<String> values() {
        return null;
    }

    public void add(String value) {
        value = value.toLowerCase();

        int index = alphaNumeric.indexOf(value.charAt(0));

        buckets.get(index).increment();

        this.entrySet.add(value);
    }

    public void removeValue(String value) {
        value = value.toLowerCase();
        int index = alphaNumeric.indexOf(value.charAt(0));

        buckets.get(index).decrement();
    }

    public int getEntriesInRange(String start, String end) {
        // Inclusive of both start and end.
        start = start.toLowerCase();
        end = end.toLowerCase();

        int startIndex = alphaNumeric.indexOf(start.charAt(0));
        int endIndex = alphaNumeric.indexOf(end.charAt(0));

        int result = 0;
        for (int i = startIndex; i <= endIndex; i++) {
            result += this.buckets.get(i).getCount();
        }

        return result;
    }

    public int getMinValueIndex() {
        int minValue = 0;

        for (int i = 0; i < this.buckets.size(); i++) {
            Bucket<String> bucket = this.buckets.get(i);

            if (bucket.getCount() > 0) {
                minValue = i;
                break;
            }
        }

        return minValue;
    }

    public int getMaxValueIndex() {
        int maxValue = this.buckets.size() - 1;

        for (int i = this.buckets.size() - 1; i >= 0; i--) {
            Bucket<String> bucket = this.buckets.get(i);

            if (bucket.getCount() > 0) {
                maxValue = i;
                break;
            }
        }

        return maxValue;
    }

    @Override
    public String getMinValue() {
        return null;
    }

    @Override
    public String getMaxValue() {
        return null;
    }

    public int getNumDistinct() {
        if (this.estimate) {
            return this.numDistinct;
        } else {
            return entrySet.size();
        }
    }
}