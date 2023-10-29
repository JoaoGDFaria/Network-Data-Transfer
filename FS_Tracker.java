import java.util.*;

public class FS_Tracker {
    private Map<String, Map<Integer, List<String>>> fileMemory;

    public FS_Tracker(){
        this.fileMemory = new HashMap<>();
    }

    public int insert_info(String fileName, Integer blockNumber, String ipNode){

        if (!fileMemory.containsKey(fileName)) {
            fileMemory.put(fileName, new HashMap<>());
        }

        Map<Integer, List<String>> blockMap = fileMemory.get(fileName);

        if (!blockMap.containsKey(blockNumber)) {
            blockMap.put(blockNumber, new ArrayList<>());
        }

        List<String> ipList = blockMap.get(blockNumber);

        ipList.add(ipNode);

        blockMap.put(blockNumber, ipList);

        fileMemory.put(fileName, blockMap);

        return 0;
    }

    @Override
    public String toString() {
        return this.fileMemory.get("A").get(1).toString();
    }
}
