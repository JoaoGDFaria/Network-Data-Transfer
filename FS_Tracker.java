import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class FS_Tracker {
    private Map<String, Map<Integer, List<String>>> fileMemory;
    private Map<String, LocalTime> timeStamps;

    public FS_Tracker(){
        this.fileMemory = new HashMap<>();
        this.timeStamps = new HashMap<>();
    }


    public int insertInfo(String fileName, Integer blockNumber, String ipNode){

        LocalTime now = LocalTime.now();

        insertTimeStamps(now, ipNode);

        if (!fileMemory.containsKey(fileName)) fileMemory.put(fileName, new HashMap<>());
        Map<Integer, List<String>> blockMap = fileMemory.get(fileName);

        if (!blockMap.containsKey(blockNumber)) blockMap.put(blockNumber, new ArrayList<>());
        List<String> ipList = blockMap.get(blockNumber);

        ipList.add(ipNode);
        blockMap.put(blockNumber, ipList);
        fileMemory.put(fileName, blockMap);

        return 1;

    }

    public void insertTimeStamps(LocalTime time, String ipNode){
        this.timeStamps.put(ipNode, time);
    }

    public void sendIPBack(String fileName, Integer blockNumber){

        Map<Integer, List<String>> blockMap = fileMemory.get(fileName);
        List<String> IPs = blockMap.get(blockNumber);

        if(IPs.size()>1) {
            String used = IPs.get(0);
            IPs.remove(0);
            IPs.add(used);
        }

        blockMap.put(blockNumber, IPs);
        fileMemory.put(fileName, blockMap);
    }

    public void deleteDisconnectedNode(String ipDisc){
        int cont = 0;
        for (Map.Entry<String, Map<Integer, List<String>>> entry : fileMemory.entrySet()){
            String fileName = entry.getKey();
            Map<Integer, List<String>> blockMap = entry.getValue();

            for (Map.Entry<Integer, List<String>> blockEntry : blockMap.entrySet()){
                Integer blockNumber = blockEntry.getKey();
                List<String> ipList = blockEntry.getValue();
                ipList.remove(ipDisc);
            }
        }

        this.timeStamps.remove(ipDisc);
    }

    public void verifyTimeStamp(){
        LocalTime now = LocalTime.now();

        for(Map.Entry<String, LocalTime> entry : timeStamps.entrySet()){
            LocalTime before = entry.getValue();
            Duration duration = Duration.between(before, now);
            long secondsDifference = duration.getSeconds();

            if(secondsDifference>3) deleteDisconnectedNode(entry.getKey());
        }
    }


    public String toString() {

        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, Map<Integer, List<String>>> entry : fileMemory.entrySet()) {
            String fileName = entry.getKey();
            Map<Integer, List<String>> blockMap = entry.getValue();

            for (Map.Entry<Integer, List<String>> blockEntry : blockMap.entrySet()) {
                Integer blockNumber = blockEntry.getKey();
                List<String> ipList = blockEntry.getValue();

                result.append("File: ").append(fileName)
                        .append(", Block: ").append(blockNumber)
                        .append(", IPs: ").append(ipList).append("\n");
            }
        }

        return result.toString();
    }
}
