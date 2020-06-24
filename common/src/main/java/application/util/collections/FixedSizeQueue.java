package application.util.collections;

import java.lang.reflect.Array;
import java.util.*;

/**
 * real-time-traf compatibility.realTraffic.gis FixedSizeQueue.java
 * <p/>
 * Copyright 2013 Xdata@SIAT
 * Created:2013-4-8 下午3:26:36
 * email: gh.chen@siat.ac.cn
 */
public class FixedSizeQueue<E> implements Queue<E> {
    private Object[] elements;
    private int capacity;
    private int head;
    private int tail;
    private int size;

    private int modCount;

    public FixedSizeQueue(int capacity) {
        if (capacity < 0)
            throw new IllegalArgumentException("Illegal Capacity: " + capacity);

        elements = new Object[capacity];
        this.capacity = capacity;
        head = 0;
        tail = (head - 1) % capacity;
        size = 0;
    }

    @Override
    public boolean add(E e) {
        modCount++;
        tail = (tail + 1) % capacity;
        elements[tail] = e;
        size = (size + 1) > capacity ? capacity : size + 1;
        head = (tail + 1 + capacity - size) % capacity;
        return true;
    }

    @Override
    public E element() {
        if (size == 0)
            throw new NoSuchElementException();
        E element = (E) elements[head];
        return element;
    }

    @Override
    public boolean offer(E e) {
        return add(e);
    }

    @Override
    public E peek() {
        if (size == 0)
            return null;
        E element = (E) elements[head];
        return element;
    }

    @Override
    public E poll() {
        modCount++;
        if (size == 0)
            return null;
        E element = (E) elements[head];
        head = (head + 1) % capacity;
        size--;
        return element;
    }

    @Override
    public E remove() {
        modCount++;
        if (size == 0)
            throw new NoSuchElementException();
        E element = (E) elements[head];
        head = (head + 1) % capacity;
        size--;
        return element;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        for (E e : c) {
            add(e);
        }
        return true;
    }

    @Override
    public void clear() {
        modCount++;
        size = 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            for (Object obj : elements) {
                if (obj == null) {
                    return true;
                }
            }
        } else {
            for (Object obj : elements) {
                if (o.equals(obj)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        if (c.size() > size) {
            return false;
        }
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Iterator<E> iterator() {
        return new Iter();
    }

    @Override
    public boolean remove(Object o) {
        modCount++;
        if (o == null) {
            for (int i = 0, j = head; i < size; i++, j = (j + 1) % capacity) {
                Object obj = elements[j];
                if (obj == null) {
                    if (j >= head) {
                        System.arraycopy(elements, head, elements, (head + 1)
                                % capacity, i);
                        head = (head + 1) % capacity;
                        size--;
                    } else {
                        System.arraycopy(elements, j + 1, elements, j, size - i
                                - 1);
                        tail = (tail - 1) % capacity;
                        size--;
                    }
                    return true;
                }
            }
        } else {
            for (int i = 0, j = head; i < size; i++, j = (j + 1) % capacity) {
                Object obj = elements[j];
                if (o.equals(obj)) {
                    if (j >= head) {
                        System.arraycopy(elements, head, elements, (head + 1)
                                % capacity, i);
                        head = (head + 1) % capacity;
                        size--;
                    } else {
                        System.arraycopy(elements, j + 1, elements, j, size - i
                                - 1);
                        tail = (tail - 1) % capacity;
                        size--;
                    }
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        int count = 0;
        Iterator<?> e = iterator();
        while (e.hasNext()) {
            if (c.contains(e.next())) {
                e.remove();
                count++;
            }
        }
        modCount += count;
        return count != 0;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        int count = 0;
        Iterator<?> e = iterator();
        while (e.hasNext()) {
            if (!c.contains(e.next())) {
                e.remove();
                count++;
            }
        }
        modCount += count;
        return count != 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Object[] toArray() {
        Object[] arr = new Object[size];
        for (int i = 0, j = head; i < size; i++, j = (j + 1) % capacity) {
            arr[i] = elements[j];
        }
        return arr;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        if (a.length < size) {
            T[] arr = (T[]) Array.newInstance(a.getClass().getComponentType(),
                    size);
            for (int i = 0, j = head; i < size; i++, j = (j + 1) % capacity) {
                arr[i] = (T) elements[j];
            }
            return arr;
        }
        for (int i = 0, j = head; i < size; i++, j = (j + 1) % capacity) {
            a[i] = (T) elements[j];
        }
        if (a.length > size) {
            a[size] = null;
        }
        return a;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0, j = head; i < size; j = (j + 1) % capacity, i++) {
            if (i != 0)
                sb.append(',').append(' ');
            sb.append(elements[j]);
        }
        sb.append(']');
        return sb.toString();
    }

    private class Iter implements Iterator<E> {
        int cursor = 0;
        int lastRet = -1;
        int expectedModCount = modCount;

        @Override
        public boolean hasNext() {
            return cursor != size();
        }

        @Override
        public E next() {
            checkForComodification();
            E next = (E) elements[(head + cursor) % capacity];
            lastRet = cursor++;
            return next;
        }

        @Override
        public void remove() {
            if (lastRet == -1)
                throw new IllegalStateException();
            checkForComodification();
            int j = (head + lastRet) % capacity;
            if (j >= head) {
                System.arraycopy(elements, head, elements, (head + 1)
                        % capacity, lastRet);
                head = (head + 1) % capacity;
                size--;
            } else {
                System.arraycopy(elements, j + 1, elements, j, size - lastRet
                        - 1);
                tail = (tail - 1) % capacity;
                size--;
            }
            if (lastRet < cursor)
                cursor--;
            lastRet = -1;
        }

        final void checkForComodification() {
            if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
        }

    }
}