package intellistream.morphstream.util.network;

import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class FindIpAddress {
    private static org.slf4j.Logger LOG = LoggerFactory.getLogger(FindIpAddress.class);
    public static String findRDMAIpAddressFromBash() {
        try {
            // 执行 Bash 命令来获取 RDMA 网卡的名称
            Process process = Runtime.getRuntime().exec("ibdev2netdev | grep Up | awk '{print $5}'");

            // 读取命令的输出
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String networkInterface = reader.readLine();  // 获取网卡名称

            if (networkInterface != null && !networkInterface.isEmpty()) {
                LOG.info("RDMA Interface: {}", networkInterface);

                // 使用 ip addr 命令获取该网卡的 IP 地址
                Process ipProcess = Runtime.getRuntime().exec("ip addr show " + networkInterface + " | grep 'inet ' | awk '{print $2}'");
                BufferedReader ipReader = new BufferedReader(new InputStreamReader(ipProcess.getInputStream()));
                String ipAddress = ipReader.readLine();  // 获取IP地址

                if (ipAddress != null && !ipAddress.isEmpty()) {
                    LOG.info("RDMA IP Address: {}", ipAddress);
                    return ipAddress;
                } else {
                    LOG.info("No IP address found for RDMA interface.");
                }
            } else {
                LOG.error("No RDMA interface found.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
