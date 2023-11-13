import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;

public class Node {
    private String ipNode;
    private boolean killNode = false;
    private String pathToFiles;
    private String defragmentMessages;
    private Socket socketTCP;
    private DatagramSocket socketUDP;
    private BufferedReader bufferedFromTracker; // Ler informação enviada pelo servidor
    private BufferedWriter bufferedToTracker; // Ler informação enviada para o servidor


    public Node(String ip, Socket socketTCP, String pathToFiles, DatagramSocket socketUDP) throws IOException{
        this.ipNode = ip;
        this.defragmentMessages = "";
        this.pathToFiles = pathToFiles;
        this.socketUDP = socketUDP;
        this.socketTCP = socketTCP;
        this.bufferedToTracker = new BufferedWriter(new OutputStreamWriter(socketTCP.getOutputStream())); // Enviar 
        this.bufferedFromTracker = new BufferedReader(new InputStreamReader(socketTCP.getInputStream())); // Receber
        sendInfoToFS_Tracker(getFilesInfo());
        keepAlive();


    }

    public String getFilesInfo(){
        String payload = "";
        String previousFileName = "";
        File infoFile = new File(this.pathToFiles);
        File[] allFiles = infoFile.listFiles();
        Arrays.sort(allFiles);
        for (File file : allFiles){
            String fName = file.getName();
            String fileName = fName.substring(0, fName.length()-8);
            char unitChar = fName.charAt(fName.length()-1);
            char decimalChar = fName.charAt(fName.length()-2);

                if (payload.equals("")){
                    payload = fileName + ":";


                    int blockNumber = (decimalChar - 'a') * 26 + (unitChar - 'a') + 1;
                    payload += blockNumber;
                    previousFileName = fileName;
                }
                else{
                    if (fileName.equals(previousFileName)) {
                        payload += ",";
                        int blockNumber = (decimalChar - 'a') * 26 + (unitChar - 'a') + 1;
                        payload += blockNumber;
                    }
                    else{
                        payload += ";"+fileName+":";
                        int blockNumber = (decimalChar - 'a') * 26 + (unitChar - 'a') + 1;
                        payload += blockNumber;
                        previousFileName = fileName;
                    }
                }
        }
        payload += ";";
        return payload;
    }

    // Envio de informação para o FS_Tracker com fragmentação de pacotes, se necessário
    public void sendInfoToFS_Tracker(String payload) throws IOException{
        int maxPayload = 40;
        int payloadSize = payload.length();
        
        if (payloadSize<=maxPayload) {
            // Mensagem não fragmentada
            String finalMessage = this.ipNode + "|" + 1 + "|" + payload;
            System.out.print("Payload Sent to Tracker: " + finalMessage + "\n\n");
            bufferedToTracker.write(finalMessage);
            bufferedToTracker.newLine();
            bufferedToTracker.flush();
        }
        else{
            int totalFragments = (int)Math.ceil((double)payloadSize/maxPayload);

            for (int i = 1; i <= totalFragments; i++){
                int start = (i * maxPayload) - maxPayload;
                int end = i * maxPayload;
                String message = "";
                if (end > payloadSize) {
                    end = payloadSize;
                    // Mensagem com fragmentação (É a última)
                    message = this.ipNode + "|" + 3 + "|" + payload.substring(start, end);
                }
                else{
                    // Mensagem com fragmentação (não é a última)
                    message = this.ipNode + "|" + 2 + "|" + payload.substring(start, end);           
                }
                System.out.println("Payload Sent to Tracker: " + message);
                bufferedToTracker.write(message);
                bufferedToTracker.newLine();
                bufferedToTracker.flush();
            }
      
        }
    }


    private Timer timer;
    public void keepAlive(){
        new Thread(() -> {
            timer = new Timer();

            TimerTask task = new TimerTask() {
                
                @Override
                public void run() {
                    try {
                        if (!killNode){
                            // Keep Alive
                            bufferedToTracker.write(ipNode + "|0");
                            bufferedToTracker.newLine();
                            bufferedToTracker.flush();  
                        }
                        
                    } catch (IOException e) {
                        System.out.println("ERROR WRITING FROM NODE");
                    }
                    
                }
            };

            timer.scheduleAtFixedRate(task, 2000, 2000);
        }).start();
    }


    public void sendMessageToTracker() throws IOException {
        new Thread(() -> {

            Scanner scanner = new Scanner(System.in);
            while(socketTCP.isConnected()  && !killNode) {
                String messageToSend = scanner.nextLine();

                if (messageToSend.equals("d")){
                    try{
                        bufferedToTracker.write(messageToSend);
                        bufferedToTracker.newLine();
                        bufferedToTracker.flush();
                        disconnectNode();  
                    }
                    catch (IOException e) {
                    }
                }
                else if(messageToSend.equals("i") || messageToSend.startsWith("GET ")){
                    try{
                        bufferedToTracker.write(messageToSend);
                        bufferedToTracker.newLine();
                        bufferedToTracker.flush();
                    }
                    catch (IOException e) {
                    }
                }
                else{
                    System.out.println("Invalid input!");
                }

            }
        }).start();
    }


    public void defragmentationFromFSTracker(String message){
        if (message.startsWith("File")){
            System.out.println(message);
            return;
        }
        int aux = 0;
        String fragment = "";
        String payload = "";
        for(int i = 0; i < message.length(); i++) {
            if (message.charAt(i)=='|'){
                aux = 1;
            }
            // Ler fragmento
            else if(aux == 0){
                fragment += message.charAt(i);
            }
            // Ler payload
            else {
                payload += message.charAt(i);
            }
        }

        this.defragmentMessages += payload;

        if (Integer.parseInt(fragment) == 0){
            System.out.println("\n\n DEFRAGMENTED MESSAGE:  " +this.defragmentMessages +"\n\n");
            getBlocksFromNodes(this.defragmentMessages);
            this.defragmentMessages = "";
        }
    }


    public void getBlocksFromNodes (String payload){
        Map<Integer, List<String>> blocksToRetreive = new HashMap<>();
        String blockNum = "";
        String ipAdress = "";
        List<String> allIps = new ArrayList<>();
        int aux = 0;
        for (int i=0; i<payload.length(); i++){

            // Sabemos o bloco
            if(payload.charAt(i) == ':'){
                aux = 1;
            }
            // Bloco termina
            else if(payload.charAt(i) == ';'){
                allIps.add(ipAdress);
                aux = 0;
                if (!blocksToRetreive.containsKey(Integer.parseInt(blockNum))) {
                    blocksToRetreive.put(Integer.parseInt(blockNum), new ArrayList<>());
                }
                blocksToRetreive.get(Integer.parseInt(blockNum)).addAll(allIps);
                blockNum = "";
                ipAdress = "";
                allIps.clear();
            }
            // Novo ip
            else if(payload.charAt(i) == ','){
                allIps.add(ipAdress);
                ipAdress = "";
            }
            // Determinar o número do bloco
            else if(aux == 0){
                blockNum += payload.charAt(i);
            }
            // Determinar o número do ip
            else{
                ipAdress += payload.charAt(i);
            }
        }

        for (Map.Entry<Integer, List<String>> entry : blocksToRetreive.entrySet()) {
            Integer key = entry.getKey();
            List<String> values = entry.getValue();

            System.out.println("Key: " + key);
            System.out.println("Values: " + values);
        }
    }


    public void disconnectNode(){
        killNode=true;
        try{
            if (bufferedToTracker != null) {
                bufferedToTracker.close();
            }
            if (bufferedFromTracker != null) {
                bufferedFromTracker.close();
            }
            if (socketTCP != null){
                this.socketTCP.close();  
            }
            if (socketUDP != null){
                this.socketUDP.close();  
            }
                    
        } catch (IOException a){
            System.out.println("ERROR CLOSING NODE");
        }
        if (timer != null) {
            timer.cancel();
            timer.purge();
        }
        System.out.println("Disconnected Sucessfully");
    }

    public void listenMessageFromTracker() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                String msgFromChat;

                while (socketTCP.isConnected() && !killNode) {
                    try {
                        msgFromChat = bufferedFromTracker.readLine();
                        defragmentationFromFSTracker(msgFromChat);
                    } catch (IOException e) {
                    }
                }
            }
        }).start();
    }



    public void listenMessageFromNode() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (socketTCP.isConnected() && !killNode) {
                    try {
                        byte[] receiveData = new byte[1024];
                        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

                        socketUDP.receive(receivePacket);
                        String responsenFromNode = new String(receivePacket.getData(), 0, receivePacket.getLength());
        
                        System.out.println("Received: " + responsenFromNode);
                    }
                    catch (Exception e) {
                    }
                }
            }
        }).start();
    }

    public void sendMessageToNode() throws IOException {
        DatagramSocket clientSocket = new DatagramSocket();
        String str = "Hello World";
        byte[] buf = str.getBytes();
        DatagramPacket p = new DatagramPacket(buf, buf.length, InetAddress.getByName(this.ipNode), 9090);
        clientSocket.send(p);
        clientSocket.close();
        System.out.println("Sent");
    }

    


    public static void main (String[] args) throws IOException{
        Socket socketTCP = new Socket("10.0.0.10",9090); //"localhost"
        String ipNode = socketTCP.getLocalAddress().toString().substring(1);

        DatagramSocket socketUDP = new DatagramSocket(9090);

        String pathToFiles;
        if (ipNode.equals("10.0.1.20")){
            pathToFiles = "/home/core/Desktop/Projeto/Node1";
        }
        else if (ipNode.equals("10.0.2.20")){
            pathToFiles = "/home/core/Desktop/Projeto/Node2";
        }
        else if (ipNode.equals("10.0.3.20")){
            pathToFiles = "/home/core/Desktop/Projeto/Node3";
        }
        else{
            pathToFiles = "/home/core/Desktop/Projeto/Node4";
        }


        System.out.println("Conexão FS Track Protocol com servidor " + socketTCP.getInetAddress().getHostAddress() + " porta 9090.\n");
        Node node = new Node(ipNode, socketTCP, pathToFiles, socketUDP);
        node.listenMessageFromTracker();
        node.sendMessageToTracker();
        
        
        node.listenMessageFromNode();
        node.sendMessageToNode();
    



        
    }

}
