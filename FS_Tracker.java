import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

public class FS_Tracker {
    private Map<String, Map<Integer, List<String>>> fileMemory;
    private Map<String, LocalTime> timeStamps;

    public FS_Tracker(){
        this.fileMemory = new HashMap<>();
        this.timeStamps = new HashMap<>();
    }


    public int insertInfo(String fileName, Integer blockNumber, String ipNode){

        insertTimeStamps(LocalTime.now(), ipNode);

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
        System.out.println("*\n"+LocalTime.now()+"\n*\n");
        System.out.println(memoryToString());
        System.out.println("========");
        System.out.println(timeToString());
        Iterator<Map.Entry<String, LocalTime>> iterator = this.timeStamps.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, LocalTime> entry = iterator.next();
            LocalTime before = entry.getValue();
            Duration duration = Duration.between(before, now);
            long secondsDifference = duration.getSeconds();

            if (secondsDifference >= 3) {
                iterator.remove();
                deleteDisconnectedNode(entry.getKey());
            }
        }
    }

    public void messageParser(String mensagem){
        String messageReceived = mensagem;
        String ipNode = "";
        String payloadLength = "";
        String payload = "";
        Integer aux = 0;


        for(int i = 0; i < messageReceived.length(); i++) {
            if (messageReceived.charAt(i)=='|'){
                aux += 1;
            }
            else if (aux == 0){
                ipNode += messageReceived.charAt(i);
            }
            else if (aux == 1){
                payloadLength += messageReceived.charAt(i);
            }
            else if(aux == 2 && Integer.parseInt(payloadLength) > 0){
                payload += messageReceived.charAt(i);
            }
        }

        Boolean flag = true;
        String currentFile = "";
        Integer currentBlock = 0;

        for(int i = 0; i < payload.length(); i++){


            if (payload.charAt(i)==',');
            else if (payload.charAt(i)==':');

            // Situação de troca de file
            else if (payload.charAt(i)==';'){
                currentFile = "";
                flag = true;
            }

            // Situação de file
            else if (flag){
                currentFile += payload.charAt(i);
                if (payload.charAt(i+1) == ':'){
                    flag = false;
                }
            }

            // Situação de bloco
            else {
                currentBlock +=  payload.charAt(i) - '0';
                if (Character.isDigit(payload.charAt(i+1))){
                    currentBlock *= 10;
                }
                else{
                    insertInfo(currentFile,currentBlock,ipNode);
                    //System.out.printf("%s - %d - %s%n", currentFile, currentBlock, ipNode);
                    currentBlock = 0;
                }
            }

        }
    }


    public String memoryToString() {

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

    public String timeToString(){

        StringBuilder result = new StringBuilder();

        for(Map.Entry<String, LocalTime> entry : timeStamps.entrySet()){
            String ipName = entry.getKey();
            String time = entry.getValue().toString();

            result.append("IP: ").append(ipName)
                    .append(", Time: ").append(time).append("\n");
        }
        if(result.length() == 0) return "VAZIO";
        return result.toString();
    }
}