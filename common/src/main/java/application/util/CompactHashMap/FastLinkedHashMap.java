package application.util.CompactHashMap;

import java.util.*;

/**
 * <profiling>Hash table and linked list implementation of the <tt>Map</tt> interface,
 * with predictable iteration order.  This implementation differs from
 * <tt>HashMap</tt> in that it maintains a doubly-linked list running through
 * all of its entries.  This linked list defines the iteration ordering,
 * which is normally the order in which keys were inserted into the map
 * (<i>insertion-order</i>).  Note that insertion order is not affected
 * if a key is <i>re-inserted</i> into the map.  (A key <tt>k</tt> is
 * reinserted into a map <tt>m</tt> if <tt>m.put(k, v)</tt> is invoked when
 * <tt>m.containsKey(k)</tt> would return <tt>true</tt> immediately prior to
 * the invocation.)
 * <profiling>
 * <profiling>This implementation spares its clients from the unspecified, generally
 * chaotic ordering provided by {@link HashMap} (and {@link Hashtable}),
 * without incurring the increased cost associated with {@link TreeMap}.  It
 * can be used to produce a copy of a map that has the same order as the
 * original, regardless of the original map's implementation:
 * <pre>
 *     void foo(Map m) {
 *         Map copy = new LinkedHashMap(m);
 *         ...
 *     }
 * </pre>
 * This technique is particularly useful if a module takes a map on input,
 * copies it, and later returns results whose order is determined by that of
 * the copy.  (Clients generally appreciate having things returned in the same
 * order they were presented.)
 * <profiling>
 * <profiling>A special  is
 * provided to create a linked hash map whose order of iteration is the order
 * in which its entries were last accessed, from least-recently accessed to
 * most-recently (<i>access-order</i>).  This kind of map is well-suited to
 * building LRU caches.  Invoking the <tt>put</tt> or <tt>get</tt> method
 * results in an access to the corresponding entry (assuming it exists after
 * the invocation completes).  The <tt>putAll</tt> method generates one entry
 * access for each mapping in the specified map, in the order that key-value
 * mappings are provided by the specified map's entry set iterator.  <i>No
 * other methods generate entry accesses.</i> In particular, operations on
 * collection-views do <i>not</i> affect the order of iteration of the backing
 * map.
 * <profiling>
 * <profiling>The {@link #removeEldestEntry(Map.Entry)} method may be overridden to
 * impose a policy for removing stale mappings automatically when new mappings
 * are added to the map.
 * <profiling>
 * <profiling>This class provides all of the optional <tt>Map</tt> operations, and
 * permits null elements.  Like <tt>HashMap</tt>, it provides constant-time
 * performance for the basic operations (<tt>add</tt>, <tt>contains</tt> and
 * <tt>remove</tt>), assuming the hash function disperses elements
 * properly among the buckets.  Performance is likely to be just slightly
 * below that of <tt>HashMap</tt>, due to the added expense of maintaining the
 * linked list, with one exception: Iteration over the collection-views
 * of a <tt>LinkedHashMap</tt> requires time proportional to the <i>fieldSize</i>
 * of the map, regardless of its capacity.  Iteration over a <tt>HashMap</tt>
 * is likely to be more expensive, requiring time proportional to its
 * <i>capacity</i>.
 * <profiling>
 * <profiling>A linked hash map has two parameters that affect its performance:
 * <i>set_executor_ready capacity</i> and <i>load factor</i>.  They are defined precisely
 * as for <tt>HashMap</tt>.  Note, however, that the penalty for choosing an
 * excessively high value for set_executor_ready capacity is less severe for this class
 * than for <tt>HashMap</tt>, as iteration times for this class are unaffected
 * by capacity.
 * <profiling>
 * <profiling><strong>Note that this implementation is not synchronized.</strong>
 * If multiple threads access a linked hash map concurrently, and at least
 * one of the threads modifies the map structurally, it <em>must</em> be
 * synchronized externally.  This is typically accomplished by
 * synchronizing on some object that naturally encapsulates the map.
 * <profiling>
 * If no such object exists, the map should be "wrapped" using the
 * {@link Collections#synchronizedMap Collections.synchronizedMap}
 * method.  This is best done at creation time, to prevent accidental
 * unsynchronized access to the map:<pre>
 *   Map m = Collections.synchronizedMap(new LinkedHashMap(...));</pre>
 * <profiling>
 * A structural modification is any operation that adds or deletes one or more
 * mappings or, in the case of access-ordered linked hash maps, affects
 * iteration order.  In insertion-ordered linked hash maps, merely changing
 * the value associated with a key that is already contained in the map is not
 * a structural modification.  <strong>In access-ordered linked hash maps,
 * merely querying the map with <tt>get</tt> is a structural
 * modification.</strong>)
 * <profiling>
 * <profiling>The iterators returned by the <tt>iterator</tt> method of the collections
 * returned by all of this class's collection view methods are
 * <em>fail-fast</em>: if the map is structurally modified at any time after
 * the iterator is created, in any way except through the iterator's own
 * <tt>remove</tt> method, the iterator will throw a {@link
 * ConcurrentModificationException}.  Thus, in the face of concurrent
 * modification, the iterator fails quickly and cleanly, rather than risking
 * arbitrary, non-deterministic behavior at an undetermined time in the future.
 * <profiling>
 * <profiling>Note that the fail-fast behavior of an iterator cannot be guaranteed
 * as it is, generally speaking, impossible to make any hard guarantees in the
 * presence of unsynchronized concurrent modification.  Fail-fast iterators
 * throw <tt>ConcurrentModificationException</tt> on a best-effort basis.
 * Therefore, it would be wrong to write a program that depended on this
 * exception for its correctness:   <i>the fail-fast behavior of iterators
 * should be used only to detect bugs.</i>
 * <profiling>
 * <profiling>This class is a member of the
 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
 * Java Collections Framework</a>.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @author Josh Bloch
 * @author Alex Yakovlev
 * @see Object#hashCode()
 * @see Collection
 * @see Map
 * @see HashMap
 * @see TreeMap
 * @see Hashtable
 * @since 1.4
 */
public class FastLinkedHashMap<K, V>
        extends QuickHashMap<K, V> {
    private static final long serialVersionUID = 3801124242820219131L;

    /**
     * The iteration ordering method for this linked hash map: <tt>true</tt>
     * for access-order, <tt>false</tt> for insertion-order.
     *
     * @serial
     */
    private final boolean accessOrder;

    /**
     * Index array:
     * it's even elements are 'before' indices
     * (index of previous element in doubly linked list),
     * odd elements are 'after' (next element index).
     */
    private transient int[] prevNext;

    /**
     * Index of the eldest map element,
     * head of the doubly linked list.
     */
    private transient int headIndex;

    /**
     * Cached Entry of the eldest map element
     * for removeEldestEntry speedup.
     */
    private transient Entry headEntry;

    /**
     * Constructs an empty insertion-ordered <tt>LinkedHashMap</tt> instance
     * with the specified set_executor_ready capacity and load factor.
     *
     * @param initialCapacity the set_executor_ready capacity
     * @param loadFactor      the load factor
     * @throws IllegalArgumentException if the set_executor_ready capacity
     *                                  is too low or the load factor is nonpositive
     */
    public FastLinkedHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
        accessOrder = false;
    }

    /**
     * Constructs an empty insertion-ordered <tt>LinkedHashMap</tt> instance
     * with the specified set_executor_ready capacity and a default load factor (0.75).
     *
     * @param initialCapacity the set_executor_ready capacity
     * @throws IllegalArgumentException if the set_executor_ready capacity is too low
     */
    public FastLinkedHashMap(int initialCapacity) {
        super(initialCapacity);
        accessOrder = false;
    }

    /**
     * Constructs an empty insertion-ordered <tt>LinkedHashMap</tt> instance
     * with the default set_executor_ready capacity (16) and load factor (0.75).
     */
    public FastLinkedHashMap() {
        super();
        accessOrder = false;
    }

    /**
     * Constructs an insertion-ordered <tt>LinkedHashMap</tt> instance with
     * the same mappings as the specified map.  The <tt>LinkedHashMap</tt>
     * instance is created with a default load factor (0.75) and an set_executor_ready
     * capacity sufficient to hold the mappings in the specified map.
     *
     * @param m the map whose mappings are to be placed in this map
     * @throws NullPointerException if the specified map is null
     */
    public FastLinkedHashMap(Map<? extends K, ? extends V> m) {
        super(m);
        accessOrder = false;
    }

    /**
     * Constructs an empty <tt>LinkedHashMap</tt> instance with the
     * specified set_executor_ready capacity, load factor and ordering mode.
     *
     * @param initialCapacity the set_executor_ready capacity
     * @param loadFactor      the load factor
     * @param accessOrder     the ordering mode - <tt>true</tt> for
     *                        access-order, <tt>false</tt> for insertion-order
     * @throws IllegalArgumentException if the set_executor_ready capacity is negative
     *                                  or the load factor is nonpositive
     */
    public FastLinkedHashMap(int initialCapacity,
                             float loadFactor,
                             boolean accessOrder) {
        super(initialCapacity, loadFactor);
        this.accessOrder = accessOrder;
    }

    /**
     * Constructor to be used in LinkedHashSet
     * it creates a new LinkedHashMap
     * storing only keys without values
     * if withValues is false.
     */
    FastLinkedHashMap(int initialCapacity,
                      float loadFactor,
                      boolean accessOrder,
                      boolean withValues) {
        super(initialCapacity, loadFactor, withValues);
        this.accessOrder = accessOrder;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <profiling>
     * <profiling>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
     * key.equals(k))}, then this method returns {@code v}; otherwise
     * it returns {@code null}.  (There can be at most one such mapping.)
     * <profiling>
     * <profiling>A return value of {@code null} does not <i>necessarily</i>
     * indicate that the map contains no mapping for the key; it's also
     * possible that the map explicitly maps the key to {@code null}.
     * The {@link #containsKey containsKey} operation may be used to
     * distinguish these two cases.
     */
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        int i = positionOf(key);
        if (i == NO_INDEX) return null;
        updateIndex(i);
        return (V) (keyIndexShift > 0 ?
                keyValueTable[(i << keyIndexShift) + 2] :
                DUMMY_VALUE);
    }

    /**
     * Removes all of the mappings from this map.
     * The map will be empty after this call returns.
     */
    public void clear() {
        super.clear();
        headIndex = NO_INDEX;
        headEntry = null;
    }

    /**
     * Increase fieldSize of internal arrays.
     */
    void resize(int newCapacity) {
        super.resize(newCapacity);
        if (prevNext != null)
            prevNext = Arrays.copyOf(prevNext, (threshold + 1) << 1);
        else if (threshold > 0)
            prevNext = new int[(threshold + 1) << 1];
    }

    /**
     * Returns a shallow copy of this <tt>LinkedHashMap</tt> instance:
     * the keys and values themselves are not cloned.
     *
     * @return a shallow copy of this map
     */
    public FastLinkedHashMap<K, V> clone() {
        FastLinkedHashMap<K, V> that = (FastLinkedHashMap<K, V>) super.clone();
        if (prevNext != null)
            that.prevNext = Arrays.copyOf(prevNext, (threshold + 1) << 1);
        that.headEntry = null;
        return that;
    }

    /**
     * Returns <tt>true</tt> if this map should remove its eldest entry.
     * This method is invoked by <tt>put</tt> and <tt>putAll</tt> after
     * inserting a new entry into the map.  It provides the implementor
     * with the opportunity to remove the eldest entry each time a new one
     * is added.  This is useful if the map represents a cache: it allows
     * the map to reduce memory consumption by deleting stale entries.
     * <profiling>
     * <profiling>Sample use: this override will allow the map to grow up to 100
     * entries and then delete the eldest entry each time a new entry is
     * added, maintaining a steady state of 100 entries.
     * <pre>
     *     private static final int MAX_ENTRIES = 100;
     *
     *     protected boolean removeEldestEntry(Map.Entry eldest) {
     *        return fieldSize() > MAX_ENTRIES;
     *     }
     * </pre>
     * <profiling>
     * <profiling>This method typically does not modify the map in any way,
     * instead allowing the map to modify itself as directed by its
     * return value.  It <i>is</i> permitted for this method to modify
     * the map directly, but if it does so, it <i>must</i> return
     * <tt>false</tt> (indicating that the map should not attempt any
     * further modification).  The effects of returning <tt>true</tt>
     * after modifying the map from within this method are unspecified.
     * <profiling>
     * <profiling>This implementation merely returns <tt>false</tt> (so that this
     * map acts like a normal map - the eldest element is never removed).
     *
     * @param eldest The least recently inserted entry in the map, or if
     *               this is an access-ordered map, the least recently accessed
     *               entry.  This is the entry that will be removed it this
     *               method returns <tt>true</tt>.  If the map was empty prior
     *               to the <tt>put</tt> or <tt>putAll</tt> invocation resulting
     *               in this invocation, this will be the entry that was just
     *               inserted; in other words, if the map contains a partition
     *               entry, the eldest entry is also the newest.
     * @return <tt>true</tt> if the eldest entry should be removed
     * from the map; <tt>false</tt> if it should be retained.
     */
    private boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return false;
    }

    /**
     * Called by superclass constructors and pseudoconstructors (clone,
     * readObject) before any entries are inserted into the map.  Initializes
     * the chain.
     */
    void init() {
        if (threshold > 0)
            prevNext = new int[(threshold + 1) << 1];
        headIndex = NO_INDEX;
        headEntry = null;
    }

    /**
     * This method is called after addition of new key/value pair.
     * <profiling>
     * Here we insert its index to the very end of the linked list
     * and check removeEldestEntry and remove map eldest pair
     * if it returns true. Doing this check after addition can
     * cause unnecessary resize.
     */
    void addHook(int i) {
        insertIndex(i);
        //
        if (headEntry == null) {
            headEntry = new Entry(headIndex);
        }
        if (removeEldestEntry(headEntry)) {
            removeKey(headEntry.getKey(), headIndex);
        }
    }

    /**
     * This method is called when key/value pair is removed from the map.
     * <profiling>
     * Here we remove its index from the linked list.
     */
    void removeHook(int i) {
        removeIndex(i);
    }

    /**
     * This method is called when existing key's value is modified.
     * <profiling>
     * Here we move its index to the end of linked list
     * if accessOrder is true and update cached HeadEntry.
     */
    @SuppressWarnings("unchecked")
    void updateHook(int i) {
        updateIndex(i);
        if (headEntry != null && headIndex == i && keyIndexShift > 0)
            headEntry.value = (V) keyValueTable[(i << keyIndexShift) + 2];
    }

    /**
     * This method is called when element is relocated
     * during defragmentation.
     */
    void relocateHook(int newIndex, int oldIndex) {
        if (size == 1) {
            prevNext[(newIndex << 1) + 2] =
                    prevNext[(newIndex << 1) + 3] = newIndex;
        } else {
            int prev = prevNext[(oldIndex << 1) + 2];
            int next = prevNext[(oldIndex << 1) + 3];
            prevNext[(newIndex << 1) + 2] = prev;
            prevNext[(newIndex << 1) + 3] = next;
            prevNext[(prev << 1) + 3] =
                    prevNext[(next << 1) + 2] = newIndex;
        }
        if (headIndex == oldIndex) {
            headIndex = newIndex;
            headEntry = null;
        }
    }

    // Iteration order based on the linked list.

    final int iterateFirst() {
        return headIndex;
    }

    final int iterateNext(int i) {
        i = prevNext[(i << 1) + 3];
        return i == headIndex ? NO_INDEX : i;
    }

    /**
     * Remove index from the linked list.
     */
    private void removeIndex(int i) {
        if (size == 0) {
            headIndex = NO_INDEX;
            headEntry = null;
        } else {
            int prev = prevNext[(i << 1) + 2];
            int next = prevNext[(i << 1) + 3];
            prevNext[(next << 1) + 2] = prev;
            prevNext[(prev << 1) + 3] = next;
            if (headIndex == i) {
                headIndex = next;
                headEntry = null;
            }
        }
    }

    /**
     * Add index to the linked list.
     *
     * @param i index
     */
    private void insertIndex(int i) {
        if (headIndex == NO_INDEX) {
            prevNext[(i << 1) + 2] =
                    prevNext[(i << 1) + 3] =
                            headIndex = i;
        } else {
            int last = prevNext[(headIndex << 1) + 2];
            prevNext[(i << 1) + 2] = last;
            prevNext[(i << 1) + 3] = headIndex;
            prevNext[(headIndex << 1) + 2] =
                    prevNext[(last << 1) + 3] = i;
        }
    }

    /**
     * Move specified index to the end of the list
     * if accessOrder is true.
     *
     * @param i index
     */
    private void updateIndex(int i) {
        if (accessOrder) {
            removeIndex(i);
            insertIndex(i);
            modCount++;
        }
    }

}
