package common.tools.cacheSim;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * Class to simulate a cache with a specified associativity and number of sets
 *
 * @author Nick Gilbert
 */
public class Cache {
    private final Set[] sets;
    private int setAssoc, hitCount, missCount, totalCount;
    private double hitRate, missRate;

    public Cache(int passedNumSets, int passedSetAssoc) {
        this.sets = new Set[passedNumSets / setAssoc];
        for (int i = 0; i < this.sets.length; i++) {
            this.sets[i] = new Set(passedSetAssoc);
        }
        this.setAssoc = passedSetAssoc;
        this.hitCount = 0;
        this.missCount = 0;
        this.totalCount = 0;
        this.hitRate = 0.0;
        this.missRate = 0.0;
    }

    /**
     * Takes a .dat file name, reads memory addresses from it, and simulates filling the cache
     * as it reads each address
     */
    public void fillFromFile(String fileName) throws IOException {
        Scanner inFile = new Scanner(new File(fileName));
        while (inFile.hasNextInt()) {
            totalCount++;
            int addressToRead = inFile.nextInt(); //Getting next byte address
            addressToRead /= 4; //Converting to a word address
            int blockAddress = addressToRead / 4;
            int location = (blockAddress % sets.length); //Location = (MemoryAddress % CacheSize)
            //System.out.println(blockAddress + ": set " + location);
            Set setToPlaceAddress = sets[location];
            boolean isHit = setToPlaceAddress.checkQueue(blockAddress);
            System.out.println(totalCount + "@" + location + ": " + sets[location]);
            if (isHit) {
                hitCount++;
            } else {
                missCount++;
            }
            System.out.println(isHit);
        }
        inFile.close();
        hitRate = hitCount / (double) totalCount * 100;
        missRate = missCount / (double) totalCount * 100;
    }

    public int getSetAssoc() {
        return setAssoc;
    }

    public void printStats() {
        System.out.println("Cache Stats!\n-----------------");
        System.out.println(this);
        System.out.println("Hit Count: " + hitCount);
        System.out.println("Miss Count: " + missCount);
        System.out.println("Hit Rate: " + hitRate);
        System.out.println("Miss Rate: " + missRate);
    }

    public String toString() {
        return "Cache Sets: " + sets.length + "\n" + "Set Associativity: " + setAssoc + "\n" + "Block Size: 4";
    }
}
