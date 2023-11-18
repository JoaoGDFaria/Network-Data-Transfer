import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private Map<String,Integer> infoProgression; // Regista as mensagens recebidas por UDP
    private FileOutputStream outputStream;


    // Para UDP

    private File infoFile;
    private byte[] fileInBytes;
    private int totalSize = 0;
    private int fragmentoAtual = -1;
    boolean isOver = false;
    boolean hasStarted = false;
    public int n_sequencia_esperado = 1;
    public int cont=0;


    public Node(String ip, Socket socketTCP, String pathToFiles, DatagramSocket socketUDP) throws IOException{
        this.ipNode = ip;
        this.defragmentMessages = "";
        this.pathToFiles = pathToFiles;
        this.socketUDP = socketUDP;
        this.socketTCP = socketTCP;
        this.outputStream = null;
        this.fileInBytes = null;
        this.infoProgression = new HashMap<>();
        this.bufferedToTracker = new BufferedWriter(new OutputStreamWriter(socketTCP.getOutputStream())); // Enviar 
        this.bufferedFromTracker = new BufferedReader(new InputStreamReader(socketTCP.getInputStream())); // Receber
        sendInfoToFS_Tracker(getFilesInfo());
        keepAlive();


    }




    ////////////////////////////////////////////////////
    ////////////////////////////////////////////////////
    //COMUNICAÇÃO TCP
    ////////////////////////////////////////////////////
    ////////////////////////////////////////////////////

    public String getFilesInfo(){
        String payload = "";
        String previousFileName = "";
        File infoFile = new File(this.pathToFiles);
        File[] allFiles = infoFile.listFiles();
        //Arrays.sort(allFiles);
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
            //System.out.print("Payload Sent to Tracker: " + finalMessage + "\n\n");  // COLOCAR ATIVO PARA DEMONSTRAR
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
                //System.out.println("Payload Sent to Tracker: " + message);  // COLOCAR ATIVO PARA DEMONSTRAR
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
                        System.out.println("Socket closed");
                        System.exit(0);
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
                        System.out.println(e.getMessage());
                    }
                }
                else if(messageToSend.equals("i") || messageToSend.startsWith("GET ")){
                    try{
                        bufferedToTracker.write(messageToSend);
                        bufferedToTracker.newLine();
                        bufferedToTracker.flush();
                    }
                    catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                }
                else{
                    System.out.println("Invalid input!");
                }

            }
            scanner.close();
        }).start();
    }

    public void listenMessageFromTracker() {
        new Thread(() -> {
            String msgFromChat;

            while (socketTCP.isConnected() && !killNode) {
                try {
                    msgFromChat = bufferedFromTracker.readLine();
                    defragmentationFromFSTracker(msgFromChat);
                } catch (IOException e) {
                    if(!killNode){
                        System.out.println("ListenMessageFromTracker ERRO: " + e.getMessage());
                    }
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
            //System.out.println("\n\n DEFRAGMENTED MESSAGE:  " +this.defragmentMessages +"\n\n");  // COLOCAR ATIVO PARA DEMONSTRAR
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

            //System.out.println("Key: " + key);  // COLOCAR ATIVO PARA DEMONSTRAR
            //System.out.println("Values: " + values);  // COLOCAR ATIVO PARA DEMONSTRAR
        }


        sendToNodes(blocksToRetreive);
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
                socketTCP.close();  
            }
            if (socketUDP != null){
                socketUDP.close();  
            }
            if (timer != null) {
                timer.cancel();
                timer.purge();
            }

        } catch (IOException a){
            System.out.println("ERROR CLOSING NODE");
        }
        finally{
            System.out.println("Disconnected Sucessfully");   
            System.exit(0);
        }
        
    }

































    ////////////////////////////////////////////////////
    ////////////////////////////////////////////////////
    //COMUNICAÇÃO UDP
    ////////////////////////////////////////////////////
    ////////////////////////////////////////////////////





    public void sendToNodes (Map<Integer, List<String>> blocksToRetreive){
        Map<String, String> messages = new HashMap<>();
        for (Map.Entry<Integer, List<String>> entry : blocksToRetreive.entrySet()) {
            String value = entry.getKey().toString(); // número de bloco do ficheiro
            String key = entry.getValue().get(0); // primeiro ip que contém o bloco
            List<String> allValues = entry.getValue(); // todos os ips que contém o bloco

            
            if (!allValues.contains(ipNode)){
                if (!messages.containsKey(key)) {
                    messages.put(key, value);
                }
                else{
                    String aux = messages.get(key)+","+value;
                    messages.put(key, aux);
                }
            }
        
        }
        for (Map.Entry<String, String> entry : messages.entrySet()) {
            String key = entry.getKey();
            String values = entry.getValue();
            try{
                sendMessageToNode("0|"+ipNode+"|"+values, key);
            }
            catch (Exception e){
                System.out.println(e.getMessage());
            }


            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(!hasStarted){
                    try {
                        sendMessageToNode("0|"+ipNode+"|"+values, key);
                        System.out.println("Resending Asking For file");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    break;
                }
            }
            hasStarted=false;
            break; // TO REMOVE LATER
            
        }
    }




    public void sendFiles(String filename, String ipToSend){
        new Thread(() -> {
            infoFile = new File(this.pathToFiles+"/"+filename); // File to send
            Path path = infoFile.toPath();
            try{
                fileInBytes = Files.readAllBytes(path);     // Converte file in bytes
            }
            catch (IOException e){
                e.printStackTrace();
            }

            try {
                sendMessageToNode("F|"+filename+"|"+fileInBytes.length, ipToSend);
            } catch (IOException e) {
                e.printStackTrace();
            }

            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(fragmentoAtual!=1){
                    try {
                        sendMessageToNode("F|"+filename+"|"+fileInBytes.length, ipToSend);
                        System.out.println("Resending ..... ACK -> "+ 0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    break;
                }
            }


            int maxPacketSize = 1021;
            int totalBytes = fileInBytes.length;
            int totalFragments = (int)Math.ceil((double)totalBytes/maxPacketSize);
            boolean isLastFragment = false;

            for (int i = 1; i <= totalFragments; i++){
                int start = (i - 1) * maxPacketSize;
                int end = i * maxPacketSize;
                

                // Mensagem com fragmentação (É a última)
                if (end > totalBytes) {
                    end = totalBytes;
                    isLastFragment = true;
                }
                byte[] eachMessage = new byte[end-start+3];
                if (isLastFragment){
                    eachMessage[0] = (byte) (1);
                }
                else{
                    eachMessage[0] = (byte) (0);
                }
                eachMessage[1] = (byte) (i & 0xFF);
                eachMessage[2] = (byte) ((i >> 8) & 0xFF);
                System.arraycopy(fileInBytes, start, eachMessage, 3, end-start);
                try {
                    sendMessageToNodeInBytes(eachMessage,ipToSend);
                    System.out.println("ACK -> "+ i);
                } catch (IOException e) {
                    e.printStackTrace();
                }



                while (true) {
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if(!isOver && i+1!=fragmentoAtual){
                        try {
                            sendMessageToNodeInBytes(eachMessage,ipToSend);
                            System.out.println("Resending ..... ACK -> "+ i);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    else{
                        break;
                    }
                }
            }
        }).start();
    }




    public void getFile(byte[] messageFragment){
        cont++;
        int last_fragment = (messageFragment[0]);
        int numero_sequencia = ((messageFragment[1] & 0xFF) | ((messageFragment[2] << 8) & 0xFF00));

        // Se o número de sequencia for o esperado, tudo corre bem
        if (numero_sequencia == n_sequencia_esperado){

            try{
                if (last_fragment == 1){
                    int size = totalSize%1021;

                    byte[] file_info = new byte[size];
                    System.arraycopy(messageFragment, 3, file_info, 0, size);
                    outputStream.write(file_info);
                    outputStream.close();
                    System.out.println("Packets received: "+ cont);
                    cont=0;
                    sendMessageToNode("FIN", "10.0.1.20");
                }
                else{
                    byte[] file_info = new byte[messageFragment.length-3];
                    System.arraycopy(messageFragment, 3, file_info, 0, file_info.length);
                    outputStream.write(file_info);
                    sendMessageToNode("ACK"+n_sequencia_esperado, "10.0.1.20");
                }
                n_sequencia_esperado++;
            }
            catch (IOException e){
                e.printStackTrace();
            }

        }
        else{
            try{
                sendMessageToNode("ACK"+(n_sequencia_esperado-1), "10.0.1.20");
            } catch (IOException e){
                
            }
        }

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
                        // First message
                        if (responsenFromNode.startsWith("0|")){
                            isOver=false;
                            System.out.println("Received: " + responsenFromNode);
                            sendFiles("KaiCenat_blocoab","10.0.2.20");
                        }
                        // Create the file
                        else if (responsenFromNode.startsWith("F|")){
                            n_sequencia_esperado = 1;
                            hasStarted=true;
                            String[] split = responsenFromNode.split("\\|");
                            System.out.println("Received FileName: " + split[1]);
                            totalSize = Integer.parseInt(split[2]);
                            File file = new File ("/home/core/Desktop/Projeto/Test/" + split[1]);
                            outputStream = new FileOutputStream(file);
                            sendMessageToNode("ACK0", "10.0.1.20");
                        }
                        else if (responsenFromNode.startsWith("ACK")){
                            int ack_num = Integer.parseInt(responsenFromNode.substring(3));
                            fragmentoAtual = ack_num+1;
                            //System.out.println("Node 1: " + responsenFromNode);
                        }
                        else if (responsenFromNode.startsWith("FIN")){
                            //System.out.println(responsenFromNode);
                            isOver = true;
                        }
                        // Fragmented messages
                        else{
                            getFile(receivePacket.getData());
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    // IP a quem eu quero enviar
    public void sendMessageToNode(String messageToSend, String ipToSend) throws IOException {
        DatagramSocket clientSocket = new DatagramSocket();
        byte[] buf = messageToSend.getBytes();
        DatagramPacket p = new DatagramPacket(buf, buf.length, InetAddress.getByName(ipToSend), 9090);
        clientSocket.send(p);
        clientSocket.close();
        System.out.println("Sent: "+messageToSend);
        //timeProgression(messageToSend, ipToSend, ack_atual);
    }



    // IP a quem eu quero enviar
    public void sendMessageToNodeInBytes(byte[] messageToSend, String ipToSend) throws IOException {
        DatagramSocket clientSocket = new DatagramSocket();
        DatagramPacket p = new DatagramPacket(messageToSend, messageToSend.length, InetAddress.getByName(ipToSend), 9090);
        clientSocket.send(p);
        clientSocket.close();
        //timeProgression(messageToSend, ipToSend, ack_atual);
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
    
        
    }

}





