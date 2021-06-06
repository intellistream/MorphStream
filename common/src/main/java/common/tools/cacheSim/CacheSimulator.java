package common.tools.cacheSim;

import java.io.IOException;
import java.util.Scanner;

/**
 * A program to simulate different types of caches for statistical analysis
 * on how different cache setups compare to each other
 * <p>
 * Nick Gilbert
 * <p>
 * NOTES: Using Byte Addresses
 * Take address from file, convert to a word address
 * 4 words per block
 * block size = 4
 * byte / 4 = word
 * word / 4 = block
 * block % rowsInCache = location
 * <p>
 * Rows = numSets
 * Cols = setAssoc
 */
//TODO FIX LRU ALGORITHM
public class CacheSimulator {
    /**
     * Main method
     */
    public static void main(String[] args) throws IOException {
        //Creating the cache
        Scanner in = new Scanner(System.in);
        int numSets, setAssoc;
        do {
            System.out.print("Enter number of cache sets (1/32/64/128/256/512): ");
            numSets = in.nextInt();
        }
        while (numSets != 1 && numSets != 32 && numSets != 64 && numSets != 128 && numSets != 256 && numSets != 512);
        do {
            System.out.print("Enter set associativity (1/2/4): ");
            setAssoc = in.nextInt();
        }
        while (setAssoc != 1 && setAssoc != 2 && setAssoc != 4);
        Cache cache = new Cache(numSets, setAssoc);
        System.out.println("Cache created!");
        //Getting file to read from
        System.out.print("Enter the filename to check: ");
        String datFile = in.next();
        //Filling cache from file
        in.close(); //End of keyboard input
        cache.fillFromFile(datFile);
        cache.printStats();
    }
}
