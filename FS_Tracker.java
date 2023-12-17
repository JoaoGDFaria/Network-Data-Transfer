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
import java.util.concurrent.locks.ReentrantLock;

/**
 * CLASS FS_TRACKER - Servidor tracker
 */
public class FS_Tracker {

    private Map<String, Map<Integer, List<String>>> fileMemory; // responsável por guardar os filenames, blocos que cada filename tem e os ips associados a cada um deles
    private Map<String, String> defragmentMessages; // responsável por guardar as mensagens que vão sendo defragmentadas por ordem até obter obter o último pacote
    private Map<String, LocalTime> timeStamps; // responsávelo por guardar os ips que estão ativos, devido ao mecanismo keepAlive

    private ReentrantLock lock = new ReentrantLock();

    public FS_Tracker(){
        this.fileMemory = new HashMap<>();
        this.timeStamps = new HashMap<>();
        this.defragmentMessages = new HashMap<>();
    }

    // A cada nova conexão de um novo node, fazer accept dessa determinada socket
    private void startFS_Tracker(Integer port) throws IOException {
        ServerSocket tracker_socket = new ServerSocket(port);
        System.out.println("Servidor ativo em 192.164.4.10 porta " + tracker_socket.getLocalPort() + ".\n");

        checkAlive();

        while (!tracker_socket.isClosed()) {
            new NodeHandler(tracker_socket.accept(), this).start();
        }
    }

    // Verificar de 3 em 3 segundos se algum nodo deixa de comunicar com o tracker, se acontecer desconectá-lo da rede
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

    // Inserir o novo timestamp do nodo
    public void insertTimeStamps(LocalTime time, String ipNode){
        lock.lock();
        try{
            this.timeStamps.put(ipNode, time);
        }finally{
            lock.unlock();
        }
    }

    // Verificar todos os timestamps atuais de cada nodo e se tiverem passado 3 segundos, então remové-lo da rede
    public void verifyTimeStamp(){
        lock.lock();
        try{
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
            lock.unlock();
        }
    }

    // Desconectar um respetivo nodo da rede
    public void deleteDisconnectedNode(String ipDisc){
        lock.lock();
        try{
            for (Map.Entry<String, Map<Integer, List<String>>> entry : fileMemory.entrySet()){
                Map<Integer, List<String>> blockMap = entry.getValue();

                for (Map.Entry<Integer, List<String>> blockEntry : blockMap.entrySet()){
                    List<String> ipList = blockEntry.getValue();
                    ipList.remove(ipDisc);
                }
            }
            timeStamps.remove(ipDisc);
        }finally{
            lock.unlock();
            System.out.println("Node " + ipDisc + " has been disconnected");
        }
    }

    // Inserir informações na hash fileMemory
    public void insertInfo(String fileName, Integer blockNumber, String ipNode){
        insertTimeStamps(LocalTime.now(), ipNode);

        if (fileName.equals("null")){
            return;
        }

        lock.lock();
        try{
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
            lock.unlock();
        }
    }

    // Descobrir o ip do nodo que se está a comunicar connosco através da mensagem inicial
    public String ipAddressNode(String mensagem){
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
        // Defragmentar mensagem
        else if (!fragmentNumber.equals("1")){
            defragmentationFromNode(ipNode, payload, fragmentNumber);
            return;
        }

        // Parse da informação a colocar na hash fileMemory
        String currentFile = "";
        String[] blocos = payload.split(";");

        for(String bloco : blocos){
            String[] info = bloco.split(":");
            currentFile = info[0];
            if(info[1].indexOf(',') != -1){
                String[] blocoInfo = info[1].split(",");
                for(String numBlock : blocoInfo){
                    insertInfo(currentFile, Integer.parseInt(numBlock), ipNode);
                }
            }else if(info[1].indexOf('-') != -1){
                String[] blo = info[1].split("-");
                int primeiro = Integer.parseInt(blo[0]);
                int ultimo = Integer.parseInt(blo[1]);
                for(int i = primeiro; i < ultimo; i++){
                    insertInfo(currentFile, i, ipNode);
                }
                insertInfo(currentFile, ultimo, ipNode);
            }else{
                insertInfo(currentFile, Integer.parseInt(info[1]), ipNode);
            }
        }
    }

    // Responsável por receber mensagens fragmentadas e inseri-las na hash table de fragmentação por ordem até receber a última mensagem
    public void defragmentationFromNode(String ipNode, String payload, String fragInfo){
        lock.lock();
        try{
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
            lock.unlock();
        }
    }

    // Procurar na hash dos ficheiros, se existe o ficheiro que o nodo pediu, se existe então retornar os pacotes, senão retornar "ERROR"
    public String pickFile(String fileName, BufferedWriter bufferedToNode) throws IOException{
        lock.lock();
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
            lock.unlock();
        }
        return messageToSend;
    }


    // Envio de informação para o FS_Tracker com fragmentação de pacotes, se necessário
    public void sendInfoToNode(String payload, BufferedWriter bufferedToNode) throws IOException{
        if (payload.equals("ERROR")){
            return;
        }
        int maxPayload = 1024;
        int payloadSize = payload.length();
        
        if (payloadSize<=maxPayload) {
            String finalMessage = "0|" + payload;
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
                if (end >= payloadSize) {
                    end = payloadSize;
                    message = "0|" + payload.substring(start, end);
                }
                else{
                    message = "1|" + payload.substring(start, end);  
                }
                
                bufferedToNode.write(message);
                bufferedToNode.newLine();
                bufferedToNode.flush();
            }
            
        }
    }

    // Print dos conteúdos da hashtable fileMemory
    public void memoryToString() {
        lock.lock();
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
                            .append(", Hosts: ").append(ipList).append("\n");
                }
            }
            if(result.length() == 0) System.out.println("VAZIO");
            else System.out.println(result.toString());
        }catch(Exception e){
            System.out.println(e.getMessage());
        }finally{
            lock.unlock();
        }
    }

    // Print dos conteúdos da hashtable timeStamps
    public void timeToString(){
        lock.lock();
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
            lock.unlock();
        }
    }

    public static void main(String args[]) throws IOException{
        FS_Tracker tracker = new FS_Tracker();
        tracker.startFS_Tracker(9090);
    }
}
