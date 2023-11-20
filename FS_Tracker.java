import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * CLASS FS_TRACKER - Servidor tracker
 */
public class FS_Tracker {

    private Map<String, Map<Integer, List<String>>> fileMemory;
    private Map<String, String> defragmentMessages;
    private Map<String, LocalTime> timeStamps;
    private Map<String, Socket> sockets;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public FS_Tracker(){
        this.fileMemory = new HashMap<>();
        this.timeStamps = new HashMap<>();
        this.defragmentMessages = new HashMap<>();
        this.sockets = new HashMap<>();
    }

    private void startFS_Tracker(Integer port) throws IOException {
        ServerSocket tracker_socket = new ServerSocket(port);
        System.out.println("Servidor ativo em 10.0.0.10 porta " + tracker_socket.getLocalPort() + ".\n");

        dns_beacon();
        checkAlive();

        while (!tracker_socket.isClosed()) {
            new NodeHandler(tracker_socket.accept(), this).start();
        }
    }

    public void dns_beacon() throws IOException{
        Socket socketDNS = new Socket("10.0.5.10", 2302);
        BufferedWriter bufferedToDNS = new BufferedWriter(new OutputStreamWriter(socketDNS.getOutputStream()));

        String request = "FSTracker" + "|" + socketDNS.getLocalAddress().toString().substring(1) + "|BEACON";
        bufferedToDNS.write(request);
        bufferedToDNS.newLine();
        bufferedToDNS.flush();
    }

    public void checkAlive(){
        new Thread(() -> {
            Timer timer = new Timer();

            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    verifyTimeStamp();
                }
            };

            timer.scheduleAtFixedRate(task, 3000, 3000);
        }).start();
    }

    public void insertTimeStamps(LocalTime time, String ipNode){
        try{
            lock.writeLock().lock();
            this.timeStamps.put(ipNode, time);
        }finally{
            lock.writeLock().unlock();
        }
    }

    public void verifyTimeStamp(){
        try{
            lock.writeLock().lock();
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
        }finally{
            lock.writeLock().unlock();
        }
    }

    public void deleteDisconnectedNode(String ipDisc){
        try{
            lock.writeLock().lock();

            if(this.sockets.containsKey(ipDisc)){
                Socket socket = this.sockets.get(ipDisc);
                this.sockets.remove(ipDisc);
                if(!socket.isClosed()){
                    try{
                        socket.close();
                    }catch (IOException a){
                        a.printStackTrace();
                    }
                }
            }

            for (Map.Entry<String, Map<Integer, List<String>>> entry : fileMemory.entrySet()){
                Map<Integer, List<String>> blockMap = entry.getValue();

                for (Map.Entry<Integer, List<String>> blockEntry : blockMap.entrySet()){
                    List<String> ipList = blockEntry.getValue();
                    ipList.remove(ipDisc);
                }
            }
            timeStamps.remove(ipDisc);
        }finally{
            lock.writeLock().unlock();
            System.out.println("Node " + ipDisc + " has been disconnected");
        }
    }

    public void insertInfo(String fileName, Integer blockNumber, String ipNode){
        insertTimeStamps(LocalTime.now(), ipNode);

        if (fileName.equals("null")){
            return;
        }

        try{
            lock.writeLock().lock();
            if (!fileMemory.containsKey(fileName)) fileMemory.put(fileName, new HashMap<>());
            Map<Integer, List<String>> blockMap = fileMemory.get(fileName);

            if (!blockMap.containsKey(blockNumber)) blockMap.put(blockNumber, new ArrayList<>());
            List<String> ipList = blockMap.get(blockNumber);

            if (!ipList.contains(ipNode)){
                ipList.add(ipNode);
                blockMap.put(blockNumber, ipList);
                fileMemory.put(fileName, blockMap);
            }
        }finally{
            lock.writeLock().unlock();
        }
    }

    public void sendIPBack(String fileName, Integer blockNumber){
        try{
            lock.writeLock().lock();
            Map<Integer, List<String>> blockMap = fileMemory.get(fileName);
            List<String> IPs = blockMap.get(blockNumber);

            if(IPs.size()>1) {
                String used = IPs.get(0);
                IPs.remove(0);
                IPs.add(used);
            }

            blockMap.put(blockNumber, IPs);
            fileMemory.put(fileName, blockMap);
        }finally{
            lock.writeLock().unlock();
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
        try{
            lock.writeLock().lock();
            if (!this.defragmentMessages.containsKey(ipNode)) this.defragmentMessages.put(ipNode, "");
            String blocksIP = this.defragmentMessages.get(ipNode);

            blocksIP+=payload;

            this.defragmentMessages.put(ipNode, blocksIP);

            if (Integer.parseInt(fragInfo) == 3){
                String finalMessage = this.defragmentMessages.get(ipNode);
                this.defragmentMessages.remove(ipNode);
                messageParser(ipNode + "|1|" +finalMessage);
            }
        }finally{
            lock.writeLock().unlock();
        }
    }

    public String pickFile(String fileName, BufferedWriter bufferedToNode) throws IOException{
        lock.readLock().lock();
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
        }finally{
            lock.readLock().unlock();
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
            //System.out.println(finalMessage);  // COLOCAR ATIVO PARA DEMONSTRAR
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
                
                //System.out.println(message);  // COLOCAR ATIVO PARA DEMONSTRAR
                bufferedToNode.write(message);
                bufferedToNode.newLine();
                bufferedToNode.flush();
            }
            
        }
    }

    public void memoryToString() {
        try{
            lock.readLock().lock();
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
            lock.readLock().unlock();
        }
    }

    public void timeToString(){
        try{
            lock.readLock().lock();
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
            lock.readLock().unlock();
        }
    }

    public void adicionaSocket(Socket socket, String ip){
        this.sockets.put(ip, socket);
    }

    public static void main(String args[]) throws IOException{
        FS_Tracker tracker = new FS_Tracker();
        tracker.startFS_Tracker(9090);
    }
}