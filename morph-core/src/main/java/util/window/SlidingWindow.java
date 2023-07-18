package util.window;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Random;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public class SlidingWindow {
    private final ArrayDeque<SlidingWindowEntry> window = new ArrayDeque<>();
    private final long length;
    private long tsStart;
    private long tsEnd;

    public SlidingWindow(long length) {
        this.length = length;
    }

    public static void main(String[] args) {
        class SlidingWindowEntryImpl implements SlidingWindowEntry {
            private final long time;

            SlidingWindowEntryImpl(long time) {
                this.time = time;
            }

            @Override
            public long getTime() {
                return time;
            }
        }
        SlidingWindow window = new SlidingWindow(5);
        int start = 1000;
        Random rnd = new Random(12345);
        for (int i = 0; i < 100; i++) {
            window.add(new SlidingWindowEntryImpl(start), entries -> {
                for (SlidingWindowEntry e : entries) {
                    //System.out.println("Removed " + e.getTime());
                }
            });
            start = start + rnd.nextInt(5);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    public void add(SlidingWindowEntry entry, SlidingWindowCallback callback) {
        //System.out.println("Adding " + entry.getTime());
        // very first entry in the window
        if (tsStart == 0l) {
            tsStart = entry.getTime();
        }
        // add the entry
        window.addLast(entry);
        // sliding window should be moved.
        if (entry.getTime() > tsEnd) {
            // update the timestamp end timestamp
            tsEnd = entry.getTime();
            // now we need to remove the entries which are expired
            long newTsStart = tsEnd - length + 1;
            ArrayList<SlidingWindowEntry> removed = new ArrayList<>();
            while (tsStart < newTsStart) {
                if (window.element().getTime() < newTsStart) {
                    removed.add(window.removeFirst());
                    tsStart = window.element().getTime();
                }
            }
            callback.remove(removed);
        }
        /*System.out.println("start:" + window.element().getTime() + "    end:" + window.getLast().getTime());
        System.out.print("{");
        for (SlidingWindowEntry e : window) {
            System.out.print(e.getTime() + ", ");
        }
        System.out.print("}\n"); */
    }
}