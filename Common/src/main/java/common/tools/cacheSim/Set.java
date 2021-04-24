package common.tools.cacheSim;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
/**
 * Class to simulate a set in a cache
 *
 * @author Nick Gilbert
 */
public class Set {
    private Queue<Integer> blocks; //Data contained in the set
    private int setLength; //Set associativity
    /**
     * Constructor
     */
    public Set(int setLength) {
        this.setLength = setLength;
        blocks = new ArrayDeque<>();
    }
    /**
     * Check if the block is already there and placing it if it is not
     */
    public boolean checkQueue(int blockAddress) {
        if (blocks.contains(blockAddress)) { //If the queue contains the address
            updateQueue(blockAddress); //Move it to the back (most recently used)
            //System.out.println(blockAddress + ": hit");
            return true; //It's a hit
        }
        insertWithLRU(blockAddress); //Insert address with LRU algorithm
        //System.out.println(blockAddress + ": miss");
        return false; //It's a miss
    }
    /**
     * Method to move address to the back of the queue
     */
    private void updateQueue(int mostRecent) {
        Iterator<Integer> queueIterator = blocks.iterator(); //Iterator to check through the queue
        while (queueIterator.hasNext()) { //Finding the matching address
            int addressToCheck = queueIterator.next();
            if (addressToCheck == mostRecent) { //When we've found it
                queueIterator.remove();  //Remove it to be readded
                break;
            }
        }
        blocks.add(mostRecent); //Re-adding it to the back
    }
    /**
     * Algorithm to remove the least recently used address and add a new one
     */
    private void insertWithLRU(int address) {
        if (blocks.size() >= setLength) { //If queue is full
            blocks.remove();
            //System.out.println(blocks.remove() + " removed"); //Remove the front one, the least recently used
        }
        blocks.add(address); //Add new one to the back
    }
    public String toString() {
        StringBuilder str = new StringBuilder("[");
        for (Integer block : blocks) { //Finding the matching address
            str.append(block).append(", ");
        }
        return str.toString();
    }
}