package storage.table.stats;

import operator.QueryPlan.PredicateOperator;
import storage.datatype.DataBox;

import java.util.Collection;

/**
 * A parametrized type that stores histograms for a given value_list.
 *
 * @param <T> the type of the histogram
 */
public interface Histogram<T> {
    /**
     * Returns the current count for the given value_list
     * If the value_list was never entered into the histogram, then the count will be null
     *
     * @param value
     * @return
     */
    Long get(T value);

    /**
     * Return all the values stored in the histogram
     *
     * @return
     */
    Collection<T> values();

    /**
     * Add a new value_list to the Histogram.
     *
     * @param value the value_list to addOperation
     */
    void add(T value);

    /**
     * Removes a value_list from the Histogram
     *
     * @param value the value_list to remove
     */
    void removeValue(T value);

    float computeReductionFactor(PredicateOperator predicate,
                                 DataBox value);

    /**
     * Get the number of values within a given range, including start and up to but not including end.
     *
     * @param start the inclusive start of the range
     * @param end   the non-inclusive end of the range
     * @return the number of values in this range
     */
    int getEntriesInRange(T start, T end);

    /**
     * @return index of bucket of the min value_list.
     */
    int getMinValueIndex();

    /**
     * @return index of bucket of the max value_list.
     */
    int getMaxValueIndex();

    @SuppressWarnings("unchecked")
    T getMinValue();

    @SuppressWarnings("unchecked")
    T getMaxValue();

    int getNumDistinct();

    Histogram<T> copyWithReduction(float reductionFactor);

    Histogram<T> copyWithPredicate(PredicateOperator predicate,
                                   DataBox value);

    int getSampleCount();
}