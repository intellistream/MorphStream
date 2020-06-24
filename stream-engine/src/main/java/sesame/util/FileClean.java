package sesame.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

public class FileClean {

    private static void SDClean() {
        try {
            int cnt = Integer.MAX_VALUE;
            Scanner sc = new Scanner(new File("C:\\Users\\tony\\Documents\\data\\app\\sd\\sensors1.dat"));
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File("C:\\Users\\tony\\Documents\\data\\app\\sd\\sensors10.dat")));
            while (sc.hasNextLine() && cnt-- > 0) {
                String line = sc.nextLine();
                String[] split = line.split("\\s+");

                StringBuilder sb = new StringBuilder();
                split[3] = String.valueOf(Integer.parseInt(split[3]) + 1000);
                for (String aSplit : split) {
                    sb.append(aSplit).append(" ");
                }
                writer.write(sb.toString() + "\n");
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void LRClean() {
        try {
            Random r = new Random();
            int cnt = Integer.MAX_VALUE;
            Scanner sc = new Scanner(new File("C:\\Users\\tony\\Documents\\data\\app\\lr\\cardatapoints-combine1.out"));
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File("C:\\Users\\tony\\Documents\\data\\app\\lr\\cardatapoints-combine9.out")));
            while (sc.hasNextLine() && cnt-- > 0) {
                String line = sc.nextLine();
                String[] split = line.split("\\s+");

                StringBuilder sb = new StringBuilder();
                split[0] = String.valueOf(Integer.parseInt(split[0]) + r.nextInt(4));//change type
                split[1] = String.valueOf(Integer.parseInt(split[1]) + 80000);//change time
                split[2] = String.valueOf(Integer.parseInt(split[2]) + r.nextInt(100));//change VID
                split[4] = String.valueOf(Integer.parseInt(split[4]) + r.nextInt(10));//change xway
                for (String aSplit : split) {
                    sb.append(aSplit).append(" ");
                }
                writer.write(sb.toString() + "\n");
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        LRClean();
    }
}
