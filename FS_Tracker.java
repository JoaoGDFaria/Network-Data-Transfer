import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

public class FS_Tracker {
    private Map<String, Map<Integer, List<String>>> fileMemory;
    private Map<String, LocalTime> timeStamps;
    private ServerSocket trackerSocket;

    public FS_Tracker(ServerSocket trackerSocket){
        this.trackerSocket = trackerSocket;
        this.fileMemory = new HashMap<>();
        this.timeStamps = new HashMap<>();
    }


    private void startFS_Tracker() throws IOException {
        checkAlive();
        while (!trackerSocket.isClosed()) {
               
            Socket socket = trackerSocket.accept();
            System.out.println("NEW CLIENT");
            NodeHandler nodeHandler = new NodeHandler(socket, this);

            Thread newThread = new Thread(nodeHandler);
            newThread.start();
        }
    }


    public void insertInfo(String fileName, Integer blockNumber, String ipNode){

        insertTimeStamps(LocalTime.now(), ipNode);

        if (!fileMemory.containsKey(fileName)) fileMemory.put(fileName, new HashMap<>());
        Map<Integer, List<String>> blockMap = fileMemory.get(fileName);

        if (!blockMap.containsKey(blockNumber)) blockMap.put(blockNumber, new ArrayList<>());
        List<String> ipList = blockMap.get(blockNumber);

        ipList.add(ipNode);
        blockMap.put(blockNumber, ipList);
        fileMemory.put(fileName, blockMap);

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
        for (Map.Entry<String, Map<Integer, List<String>>> entry : fileMemory.entrySet()){
            Map<Integer, List<String>> blockMap = entry.getValue();

            for (Map.Entry<Integer, List<String>> blockEntry : blockMap.entrySet()){
                List<String> ipList = blockEntry.getValue();
                ipList.remove(ipDisc);
            }
        }

        this.timeStamps.remove(ipDisc);
    }

    public void verifyTimeStamp(){
        LocalTime now = LocalTime.now();
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

    public String ipAdressNode(String mensagem){
        String ipNode = "";
        for(int i = 0; i < mensagem.length(); i++) {
            if (mensagem.charAt(i)=='|'){
                break;
            }
            else{
                ipNode += mensagem.charAt(i);
            }
        }
        return ipNode;
    }


    // Recebe a mensagem do cliente
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

    public void pickFile(String fileName){
        Map<Integer, List<String>> blockMap = this.fileMemory.get(fileName);
        for (Map.Entry<Integer, List<String>> entry : blockMap.entrySet()){
            String entrada = entry.getValue().get(0);
            //System.out.println(entrada + " tem o bloco " + entry.getKey());
            //
            // FALTA FAZER FUNÇÃO DE TRANSFERÊNCIA
            // DO BLOCO DE NODE PARA NODE
            //
            sendIPBack(fileName, entry.getKey());
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


    public void checkAlive(){
        Timer timer = new Timer();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                verifyTimeStamp();
            }
        };

        timer.scheduleAtFixedRate(task, 0, 3000);
    }

    public static void main (String[] args) throws IOException{

        ServerSocket trackerSocket = new ServerSocket(1234);
        FS_Tracker fs = new FS_Tracker(trackerSocket);
        fs.startFS_Tracker();
    }
}
