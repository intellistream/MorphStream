package util.window;

import java.util.List;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public interface SlidingWindowCallback {
    void remove(List<SlidingWindowEntry> entries);
}