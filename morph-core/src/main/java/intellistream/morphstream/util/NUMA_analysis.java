package intellistream.morphstream.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;

public class NUMA_analysis {
    public static void main(String[] arg) {
        try {
            int app = 4;
            Scanner sc = new Scanner(new File(app + "\\thread.csv"));
            LinkedList<record> r_list = new LinkedList<>();
            while (sc.hasNextLine()) {
                String[] rw = sc.nextLine().replaceAll("\"", "").split(",");
                r_list.add(new record(rw));
            }
            HashMap<Long, String> ID_Thread = new HashMap<>();
            sc = new Scanner(new File(app + "\\thread.txt"));
            while (sc.hasNextLine()) {
                String read = sc.nextLine();
                if (read.startsWith("\"")) {
                    String[] rw_read1 = read.split("\"");
                    String[] rw_read2 = rw_read1[2].split(" ");
                    for (String rw : rw_read2)
                        if (rw.startsWith("nid="))
                            ID_Thread.put(Long.parseLong(rw.split("=")[1].substring(2), 16), rw_read1[1]);
                }
            }
            for (record r : r_list) {
                r.name = ID_Thread.get(r.TID);
            }
            //sort by CPU_CLK_UNHALTED first
            r_list.sort(Collections.reverseOrder((o1, o2) -> (o1.L2_Hit.compareTo(o2.L2_Hit))));
            for (record r : r_list) {
                System.out.println(r.name + "," + r.RMA + "," + r.RMA_Stalls);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    static class record {
        //0,1,2,3,10,45
        String name;
        Long CPU_CLK_UNHALTED;
        Long OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_ANY_RESPONSE_1;
        Long OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_LOCAL_DRAM_0;
        Long OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_REMOTE_HITM_HIT_FORWARD_1;
        Long TID;
        Long RMA = 0L;
        Long L2_Hit;
        Long L3_Hit;
        double RMA_Stalls = 0;

        record(String[] rw_record) {
            name = rw_record[0];
            CPU_CLK_UNHALTED = Long.parseLong(rw_record[1]);
            OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_ANY_RESPONSE_1 = Long.parseLong(rw_record[2]);
            OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_LOCAL_DRAM_0 = Long.parseLong(rw_record[3]);
            OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_REMOTE_HITM_HIT_FORWARD_1 = Long.parseLong(rw_record[10]);
            L2_Hit = Long.parseLong(rw_record[20]);
            L3_Hit = Long.parseLong(rw_record[21]);
            TID = Long.parseLong(rw_record[45]);
            if (CPU_CLK_UNHALTED != 0) {
                RMA = OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_ANY_RESPONSE_1 - OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_LOCAL_DRAM_0 - OFFCORE_RESPONSE_ALL_DEMAND_MLC_PREF_READS_LLC_MISS_REMOTE_HITM_HIT_FORWARD_1;
                RMA_Stalls = 108 * (double) RMA / CPU_CLK_UNHALTED;
            }
        }
    }
}
