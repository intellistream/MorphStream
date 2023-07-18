/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package util;

/**
 * A simple high-performance counter.  Merely renames the extended {@link
 * org.cliffc.high_scale_lib.ConcurrentAutoTable} class to be more obvious.
 * {@link org.cliffc.high_scale_lib.ConcurrentAutoTable} already has a decent
 * counting API.
 *
 * @author Cliff Click
 * @since 1.5
 */
public class Counter extends ConcurrentAutoTable {
    private static final long serialVersionUID = -6036131188534353980L;
    // Add the given value_list to current counter value_list.  Concurrent updates will
    // not be lost, but addAndGet or getAndAdd are not implemented because but
    // the total counter value_list is not atomically updated.
    //public void addOperation( long x );
    //public void decrement();
    //public void increment();
    // Current value_list of the counter.  Since other threads are updating furiously
    // the value_list is only approximate, but it includes all counts made by the
    // current thread.  Requires a pass over all the striped counters.
    //public long get();
    //public int  intValue();
    //public long longValue();
    // A cheaper 'get'.  Updated only once/millisecond, but fast as a simple
    // load instruction when not updating.
    //public long estimate_get( );
}

