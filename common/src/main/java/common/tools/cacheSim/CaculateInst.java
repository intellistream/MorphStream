package common.tools.cacheSim;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Scanner;
/**
 * Created by szhang026 on 4/23/2016.
 */
public class CaculateInst {
    public static String tail(File file) {
        RandomAccessFile fileHandler = null;
        try {
            fileHandler = new RandomAccessFile(file, "r");
            long fileLength = fileHandler.length() - 1;
            StringBuilder sb = new StringBuilder();
            for (long filePointer = fileLength; filePointer != -1; filePointer--) {
                fileHandler.seek(filePointer);
                int readByte = fileHandler.readByte();
                if (readByte == 0xA) {
                    if (filePointer == fileLength) {
                        continue;
                    }
                    break;
                } else if (readByte == 0xD) {
                    if (filePointer == fileLength - 1) {
                        continue;
                    }
                    break;
                }
                sb.append((char) readByte);
            }
            String lastLine = sb.reverse().toString();
            return lastLine;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (fileHandler != null)
                try {
                    fileHandler.close();
                } catch (IOException ignored) {
                    /* ignore */
                }
        }
    }
    public static int Compare(String s1, String s2) {
        String small, large;
        if (s1.length() > s2.length()) {
            small = s2;
            large = s1;
        } else {
            small = s1;
            large = s2;
        }
        int index = 0;
        for (char c : large.toCharArray()) {
            if (index == small.length()) break;
            if (c != small.charAt(index)) break;
            index++;
        }
        if (index == 0)
            System.out.println("" + s1 + " and " + s2 + " have no common prefix");
//        else
//            System.out.println("Common prefix:" + large.substring(0, index));
        return index;
    }
    public static int calculate(String filename) throws FileNotFoundException {
        File asm = new File(filename);
        if (!asm.exists() || asm.length() == 0) {
            return 0;
        }
        Scanner sc = new Scanner(asm);
        sc.nextLine();
        String r;
        String[] r_a;
        String pre_address = "";
        String cur_address = "";
        r = sc.nextLine();
        r_a = r.split(",");
        String last = tail(asm);
        int instruction_size = 0;
        while (sc.hasNext()) {
            r = sc.nextLine();
            r_a = r.split(",");
            System.out.println(r_a.length);
            if (!r_a[44].isEmpty()) {
                // System.out.println(r_a[0]);
                if (pre_address == "") {
                    pre_address = r_a[0].replaceAll("\"", "");
                } else {
                    cur_address = r_a[0].replaceAll("\"", "");
                    int choop_pos = Compare(cur_address, pre_address);
                    int change = Integer.parseInt(cur_address.substring(choop_pos), 16) - Integer.parseInt(pre_address.substring(choop_pos), 16);
                    instruction_size += change;
                    if (change < 0) {
                        System.out.println("Wrong");
                    }
                    pre_address = cur_address;
                }
            } else {
                if (!pre_address.isEmpty()) {
                    cur_address = r_a[0].replaceAll("\"", "");
                    int choop_pos = Compare(cur_address, pre_address);
                    int change = Integer.parseInt(cur_address.substring(choop_pos), 16) - Integer.parseInt(pre_address.substring(choop_pos), 16);
                    instruction_size += change;
                    pre_address = "";
                    if (change < 0) {
                        System.out.println("Wrong");
                    }
                }
            }
        }
        if (!pre_address.isEmpty()) {
            instruction_size += 4;//the last instruction is valid.
        }
        //System.out.println(instruction_size);
        return instruction_size;
    }
    public static void main(String[] arg) throws FileNotFoundException {
        calculate("C:\\Users\\szhang026\\Documents\\compatibility-app\\src\\InstructionInspection\\wc\\ReflectionFactory.asm");
    }
}
