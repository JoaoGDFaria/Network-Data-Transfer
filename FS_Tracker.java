import java.io.BufferedWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FS_Tracker {
    private Map<String, Map<Integer, List<String>>> fileMemory;
    private Map<String, String> defragmentMessages;
    private Map<String, LocalTime> timeStamps;

    private ReadWriteLock lock = new ReentrantReadWriteLock();
    Lock writelock = lock.writeLock();
    Lock readlock = lock.readLock();

    public FS_Tracker(ServerSocket trackerSocket){
        this.fileMemory = new HashMap<>();
        this.timeStamps = new HashMap<>();
        this.defragmentMessages = new HashMap<>();
    }


    private void startFS_Tracker(ServerSocket trackerSocket) throws IOException {
        System.out.println("Servidor ativo em 10.0.0.10 porta " + trackerSocket.getLocalPort() + ".\n");
        checkAlive();
        while (!trackerSocket.isClosed()) {
               
            Socket socket = trackerSocket.accept();
            NodeHandler nodeHandler = new NodeHandler(socket, this);
            

            Thread newThread = new Thread(nodeHandler);
            newThread.start();
        }
    }


    public void insertInfo(String fileName, Integer blockNumber, String ipNode){
        insertTimeStamps(LocalTime.now(), ipNode);
        if (fileName.equals("null")){
            return;
        }

        if (!fileMemory.containsKey(fileName)) fileMemory.put(fileName, new HashMap<>());
        Map<Integer, List<String>> blockMap = fileMemory.get(fileName);

        if (!blockMap.containsKey(blockNumber)) blockMap.put(blockNumber, new ArrayList<>());
        List<String> ipList = blockMap.get(blockNumber);

        if (!ipList.contains(ipNode)){
            ipList.add(ipNode);
            blockMap.put(blockNumber, ipList);
            fileMemory.put(fileName, blockMap);
        }
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
        writelock.lock();
        for (Map.Entry<String, Map<Integer, List<String>>> entry : fileMemory.entrySet()){
            Map<Integer, List<String>> blockMap = entry.getValue();

            for (Map.Entry<Integer, List<String>> blockEntry : blockMap.entrySet()){
                List<String> ipList = blockEntry.getValue();
                ipList.remove(ipDisc);
            }
        }
        this.timeStamps.remove(ipDisc);
        writelock.unlock();
        System.out.println("Node " + ipDisc + " has been disconnected");
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
        String ipNode = "";
        String payload = "";
        String fragmentNumber = "";
        Integer aux = 0;

        for(int i = 0; i < mensagem.length(); i++) {
            if (mensagem.charAt(i)=='|'){
                aux += 1;
            }
            // Ler IP
            else if (aux == 0){
                ipNode += mensagem.charAt(i);
            }
            // Ler fragmentação
            else if (aux == 1){
                fragmentNumber += mensagem.charAt(i);
            }
            // Ler payload
            else if(aux == 2 && Integer.parseInt(fragmentNumber) > 0){
                payload += mensagem.charAt(i);
            }
        }
        // Keep Alive
        if (fragmentNumber.equals("0")){
            insertInfo("null", 0, ipNode);
            return;
        } 
        // Defragment message
        else if (!fragmentNumber.equals("1")){
            defragmentationFromNode(ipNode, payload, fragmentNumber);
            return;
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
                    // System.out.printf("%s - %d - %s%n", currentFile, currentBlock, ipNode);
                    currentBlock = 0;
                }
            }

        }
    }


    public void defragmentationFromNode(String ipNode, String payload, String fragInfo){

        if (!this.defragmentMessages.containsKey(ipNode)) this.defragmentMessages.put(ipNode, "");
        String blocksIP = this.defragmentMessages.get(ipNode);

        blocksIP+=payload;

        this.defragmentMessages.put(ipNode, blocksIP);

        if (Integer.parseInt(fragInfo) == 3){
            String finalMessage = this.defragmentMessages.get(ipNode);
            this.defragmentMessages.remove(ipNode);
            messageParser(ipNode + "|1|" +finalMessage);
        }
        
    }

    public String pickFile(String fileName, BufferedWriter bufferedToNode) throws IOException{
        readlock.lock();
        String messageToSend = "";
        try{    
            Map<Integer, List<String>> blockMap = this.fileMemory.get(fileName);
            if (blockMap == null){
                bufferedToNode.write("File " + fileName + " was not found!");
                bufferedToNode.newLine();
                bufferedToNode.flush();
                return "ERROR";
            } 
            for (Map.Entry<Integer, List<String>> entry : blockMap.entrySet()){
                int blockNumber = entry.getKey();
                List<String> ipAddr = entry.getValue();

                if (!ipAddr.isEmpty()){
                    messageToSend += blockNumber + ":" + String.join(",", ipAddr) + ";";
                    }
                }
        }catch(Exception e){
            System.out.println(e.getMessage());
        }finally{
            readlock.unlock();
        }
        return messageToSend;
    }


    
    // Envio de informação para o FS_Tracker com fragmentação de pacotes, se necessário
    public void sendInfoToNode(String payload, BufferedWriter bufferedToNode) throws IOException{
        if (payload.equals("ERROR")){
            return;
        }
        int maxPayload = 30;
        int payloadSize = payload.length();
        
        if (payloadSize<=maxPayload) {
            String finalMessage = "0|" + payload;
            System.out.println(finalMessage);
            bufferedToNode.write(finalMessage);
            bufferedToNode.newLine();
            bufferedToNode.flush();
        }
        else{
            int totalFragments = (int)Math.ceil((double)payloadSize/maxPayload);

            for (int i = 1; i <= totalFragments; i++){
                int start = (i * maxPayload) - maxPayload;
                int end = i * maxPayload;
                String message;
                if (end > payloadSize) {
                    end = payloadSize;
                    message = "0|" + payload.substring(start, end);
                }
                else{
                    message = "1|" + payload.substring(start, end);  
                }
                
                System.out.println(message);
                bufferedToNode.write(message);
                bufferedToNode.newLine();
                bufferedToNode.flush();
            }
            
        }
    }





    public void memoryToString() {
        readlock.lock();
        try{
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
            if(result.length() == 0) System.out.println("VAZIO");
            else System.out.println(result.toString());
        }catch(Exception e){
            System.out.println(e.getMessage());
        }finally{
            readlock.unlock();
        }
    }

    public void timeToString(){
        readlock.lock();
        try{
            StringBuilder result = new StringBuilder();

            for(Map.Entry<String, LocalTime> entry : timeStamps.entrySet()){
                String ipName = entry.getKey();
                String time = entry.getValue().toString();

                result.append("IP: ").append(ipName)
                        .append(", Time: ").append(time).append("\n");
            }
            if(result.length() == 0) System.out.println("VAZIO");
            else System.out.println(result.toString());
        }catch(Exception e){
            System.out.println(e.getMessage());
        }finally{
            readlock.unlock();
        }
    }


    public void checkAlive(){
        Timer timer = new Timer();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                verifyTimeStamp();
            }
        };

        timer.scheduleAtFixedRate(task, 3000, 3000);
    }

    public static void main (String[] args) throws IOException{

        ServerSocket trackerSocket = new ServerSocket(9090);
        FS_Tracker fs = new FS_Tracker(trackerSocket);
        fs.startFS_Tracker(trackerSocket);
    }
}
