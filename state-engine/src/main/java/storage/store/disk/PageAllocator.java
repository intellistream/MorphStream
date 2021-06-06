package storage.store.disk;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A PageAllocation system for an OS paging system. Provides memory-mapped paging from the OS, an
 * interface to individual pages with the Page objects, an LRU cache for pages, 16GB worth of paging,
 * and virtual page translation.
 * <p>
 * YOU SHOULD NOT NEED TO CHANGE ANY OF THE CODE IN THIS PACKAGE.
 */
public class PageAllocator implements Iterable<Page>, Closeable {
    private static final int numHeaderPages = 1024;
    private static final int cacheSize = 1024;
    private static final AtomicInteger pACounter = new AtomicInteger(0);
    private static final LRUCache<Long, Page> pageLRU = new LRUCache<>(cacheSize);
    private static final AtomicLong numIOs = new AtomicLong(0);
    private static final AtomicLong cacheMisses = new AtomicLong(0);
    private final FileChannel fc;
    private final int allocID;
    private final boolean durable;
    private Page masterPage;
    private int numPages;

    /**
     * Create a new PageAllocator that writes its bytes into a file named fName. If wipe is true, the
     * data in the page is completely removed.
     *
     * @param fName the name of the file for this PageAllocator
     * @param wipe  a boolean specifying whether to wipe the file
     */
    public PageAllocator(String fName, boolean wipe) {
        this(fName, wipe, true);
    }

    public PageAllocator(String fName, boolean wipe, boolean durable) {
        this.durable = durable;
        try {
            this.fc = new RandomAccessFile(fName, "rw").getChannel();
        } catch (IOException e) {
            throw new PageException("Could not open File: " + e.getMessage());
        }
        this.masterPage = new Page(this.fc, 0, -1);
        this.allocID = pACounter.getAndIncrement();
        if (wipe) {
            // Nukes masterPage and headerPages
            byte[] masterBytes = this.masterPage.readBytes();
            IntBuffer ib = ByteBuffer.wrap(masterBytes).asIntBuffer();
            int[] pageCounts = new int[ib.capacity()];
            ib.get(pageCounts);
            this.numPages = 0;
            for (int i = 0; i < numHeaderPages; i++) {
                if (pageCounts[i] > 0) {
                    getHeadPage(i).wipe();
                }
            }
            this.masterPage.wipe();
        }
        byte[] masterBytes = masterPage.readBytes();
        IntBuffer ib = ByteBuffer.wrap(masterBytes).asIntBuffer();
        int[] pageCounts = new int[ib.capacity()];
        ib.get(pageCounts);
        this.numPages = 0;
        for (int i = 0; i < numHeaderPages; i++) {
            this.numPages += pageCounts[i];
        }
    }

    public static long getNumIOs() {
        return PageAllocator.numIOs.get();
    }

    static void incrementNumIOs() {
        PageAllocator.numIOs.getAndIncrement();
    }

    static void incrementCacheMisses() {
        PageAllocator.cacheMisses.getAndIncrement();
    }

    public static long getNumCacheMisses() {
        return PageAllocator.cacheMisses.get();
    }

    static private int translateAllocator(long vPageNum) {
        return (int) ((vPageNum & 0xFFFFFFFF00000000L) >> 32);
    }

    /**
     * Allocates a new page in the file.
     *
     * @return the virtual page number of the page
     */
    public int allocPage() {
        byte[] masterBytes = this.masterPage.readBytes();
        IntBuffer ib = ByteBuffer.wrap(masterBytes).asIntBuffer();
        int[] pageCounts = new int[ib.capacity()];
        ib.get(pageCounts);
        Page headerPage = null;
        int headerIndex = -1;
        for (int i = 0; i < numHeaderPages; i++) {
            if (pageCounts[i] < Page.pageSize) {
                // Found header page with space
                headerPage = getHeadPage(i);
                headerIndex = i;
                break;
            }
        }
        if (headerPage == null) {
            throw new PageException("No free Pages Available");
        }
        byte[] headerBytes = headerPage.readBytes();
        int pageIndex = -1;
        for (int i = 0; i < Page.pageSize; i++) {
            if (headerBytes[i] == 0) {
                pageIndex = i;
                break;
            }
        }
        if (pageIndex == -1) {
            throw new PageException("Header page should have free page but doesnt");
        }
        int newCount = pageCounts[headerIndex] + 1;
        byte[] newCountBytes = ByteBuffer.allocate(4).putInt(newCount).array();
        this.masterPage.writeBytes(headerIndex * 4, 4, newCountBytes);
        headerPage.writeByte(pageIndex, (byte) 1);
        if (this.durable) {
            this.masterPage.flush();
            headerPage.flush();
        }
        int pageNum = headerIndex * Page.pageSize + pageIndex;
        fetchPage(pageNum).wipe();
        this.numPages += 1;
        return pageNum;
    }

    /**
     * Fetches the page corresponding to virtual page number pageNum.
     *
     * @param pageNum the virtual page number
     * @return a Page object wrapping the page corresponding to pageNum
     */
    public Page fetchPage(int pageNum) {
        if (pageNum < 0) {
            throw new PageException("invalid page number -- out of bounds");
        }
        numIOs.getAndIncrement();
        synchronized (PageAllocator.class) {
            if (pageLRU.containsKey(translatePageNum(pageNum))) {
                return pageLRU.get(translatePageNum(pageNum));
            }
        }
        int headPageIndex = pageNum / Page.pageSize;
        if (headPageIndex >= numHeaderPages) {
            throw new PageException("invalid page number -- out of bounds");
        }
        byte[] headCountBytes = this.masterPage.readBytes(headPageIndex * 4, 4);
        int headCount = ByteBuffer.wrap(headCountBytes).getInt();
        if (headCount < 1) {
            throw new PageException("invalid page number -- page not allocated");
        }
        Page headPage = getHeadPage(headPageIndex);
        int dataPageIndex = pageNum % Page.pageSize;
        byte validByte = headPage.readByte(dataPageIndex);
        if (validByte == 0) {
            throw new PageException("invalid page number -- page not allocated");
        }
        int dataBlockID = 2 + headPageIndex * (Page.pageSize + 1) + dataPageIndex;
        Page dataPage = new Page(this.fc, dataBlockID, pageNum, this.durable);
        synchronized (PageAllocator.class) {
            pageLRU.put(translatePageNum(pageNum), dataPage);
        }
        return dataPage;
    }

    /**
     * Frees the page to be returned back to the system. The page is no longer valid and can be re-used
     * the next time the user called allocPage.
     *
     * @param p the page to free
     * @return whether or not the page was freed
     */
    public boolean freePage(Page p) {
        if (this.durable) {
            p.flush();
        }
        int pageNum = p.getPageNum();
        int headPageIndex = pageNum / Page.pageSize;
        int dataPageIndex = pageNum % Page.pageSize;
        Page headPage = getHeadPage(headPageIndex);
        if (headPage.readByte(dataPageIndex) == 0) {
            return false;
        }
        headPage.writeByte(dataPageIndex, (byte) 0);
        if (this.durable) {
            headPage.flush();
        }
        byte[] countBytes = masterPage.readBytes(4 * headPageIndex, 4);
        int oldCount = ByteBuffer.wrap(countBytes).getInt();
        int newCount = oldCount - 1;
        byte[] newCountBytes = ByteBuffer.allocate(4).putInt(newCount).array();
        masterPage.writeBytes(headPageIndex * 4, 4, newCountBytes);
        if (this.durable) {
            masterPage.flush();
        }
        synchronized (PageAllocator.class) {
            pageLRU.remove(translatePageNum(pageNum));
        }
        this.numPages -= 1;
        return true;
    }

    /**
     * Frees the page to be returned back to the system. The page is no longer valid and can be re-used
     * the next time the user called allocPage.
     *
     * @param pageNum the virtual page number to be flushed
     * @return whether or not the page was freed
     */
    public boolean freePage(int pageNum) {
        Page p;
        try {
            p = fetchPage(pageNum);
        } catch (PageException e) {
            return false;
        }
        return freePage(p);
    }

    /**
     * Close this PageAllocator.
     */
    public void close() {
        if (this.masterPage == null) {
            return;
        }
        if (this.durable) {
            this.masterPage.flush();
        }
        List<Long> toRemove = new ArrayList<>();
        Set<Long> vPageNums = null;
        List<Page> toFlush = new ArrayList<>();
        synchronized (PageAllocator.class) {
            vPageNums = pageLRU.keySet();
        }
        for (Long l : vPageNums) {
            if (translateAllocator(l) == this.allocID) {
                toRemove.add(l);
            }
        }
        synchronized (PageAllocator.class) {
            for (Long vPageNum : toRemove) {
                if (pageLRU.containsKey(vPageNum)) {
                    toFlush.add(pageLRU.get(vPageNum));
                    pageLRU.remove(vPageNum);
                }
            }
        }
        if (this.durable) {
            for (Page p : toFlush) {
                p.flush();
            }
        }
        this.masterPage = null;
        try {
            this.fc.close();
        } catch (IOException e) {
            throw new PageException("Could not clean Page Alloc " + e.getMessage());
        }
    }

    private Page getHeadPage(int headIndex) {
        int headBlockID = 1 + headIndex * (Page.pageSize + 1);
        return new Page(this.fc, headBlockID, -1);
    }

    public int getNumPages() {
        return this.numPages;
    }

    private long translatePageNum(int pageNum) {
        return (((long) this.allocID) << 32) | (((long) pageNum) & 0xFFFFFFFFL);
    }

    /**
     * @return an iterator of the valid pages managed by this PageAllocator.
     */
    public Iterator<Page> iterator() {
        return new PageIterator();
    }

    private class PageIterator implements Iterator<Page> {
        private int pageNum;
        private int cursor;

        public PageIterator() {
            this.pageNum = 0;
            this.cursor = 0;
        }

        public boolean hasNext() {
            return this.pageNum < PageAllocator.this.numPages;
        }

        public Page next() {
            if (this.hasNext()) {
                while (true) {
                    Page p;
                    try {
                        p = PageAllocator.this.fetchPage(cursor);
                        cursor++;
                        pageNum++;
                        return p;
                    } catch (PageException e) {
                        cursor++;
                    }
                }
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
