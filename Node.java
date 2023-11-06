import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.*;

public class Node {
    private String ipNode;
    private List<String> files;
    private boolean killNode = false;
    private String pathToFiles;
    private List<String> defragmentMessages;
    private Socket socket;
    private BufferedReader bufferedFromTracker; // Ler informação enviada pelo servidor
    private BufferedWriter bufferedToTracker; // Ler informação enviada para o servidor


    public Node(String ip, Socket socket, String info, String pathToFiles) throws IOException{
        this.ipNode = ip;
        this.defragmentMessages = new ArrayList<>();
        this.pathToFiles = pathToFiles;
        this.socket = socket;
        this.bufferedToTracker = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); // Enviar 
        this.bufferedFromTracker = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Receber
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
            String fileName = fName.substring(0, fName.length()-2);
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
        System.out.println(payload);
        return payload;
    }

    // Envio de informação para o FS_Tracker com fragmentação de pacotes, se necessário
    public void sendInfoToFS_Tracker(String payload) throws IOException{
        int maxPayload = 100;
        int payloadSize = payload.length();
        
        if (payloadSize<=maxPayload) {
            String finalMessage = this.ipNode + "|" + payloadSize + "|" + 0 + "|" + payload;
            bufferedToTracker.write(finalMessage);
            bufferedToTracker.newLine();
            bufferedToTracker.flush();
        }
        else{
            int totalFragments = (int)Math.ceil((double)payloadSize/maxPayload);

            for (int i = 1; i <= totalFragments; i++){
                int start = (i * maxPayload) - maxPayload;
                int end = i * maxPayload;
                if (end > payloadSize) {
                    end = payloadSize;
                }
                String message = this.ipNode + "|" + (end-start) + "|" + i + "/" + totalFragments + "|" + payload.substring(start, end);
                System.out.println(message);
                bufferedToTracker.write(message);
                bufferedToTracker.newLine();
                bufferedToTracker.flush();
            }
            
        }
    }


    private Timer timer;
    public void keepAlive(){
        timer = new Timer();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    if (!killNode){
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
    }


    public void sendMessageToTracker() throws IOException {

        Scanner scanner = new Scanner(System.in);
        while(socket.isConnected()  && !killNode) {
            String messageToSend = scanner.nextLine();

            if (messageToSend.equals("d")){
                bufferedToTracker.write(messageToSend);
                bufferedToTracker.newLine();
                bufferedToTracker.flush();
                disconnectNode();
            }
            else if(messageToSend.equals("i") || messageToSend.startsWith("GET ")){
                bufferedToTracker.write(messageToSend);
                bufferedToTracker.newLine();
                bufferedToTracker.flush();
            }
            else{
                System.out.println("Invalid input!");
            }

        }
    }


    public void defragmentationFromFSTracker(String message){
        int aux = 0;
        String fragment = "";
        String fragmentMax = "";
        String payload = "";
        for(int i = 0; i < message.length(); i++) {
            if (message.charAt(i)=='|'){
                aux = 2;
            }
            else if (message.charAt(i) == '/'){
                aux = 1;
            }
            else if(aux == 0){
                fragment += message.charAt(i);
            }
            else if(aux == 1){
                fragmentMax += message.charAt(i);
            }
            else {
                payload += message.charAt(i);
            }
        }

        if (this.defragmentMessages.isEmpty()){
            this.defragmentMessages = new ArrayList<>(Collections.nCopies(Integer.parseInt(fragmentMax), null));
        } 

        this.defragmentMessages.set(Integer.parseInt(fragment)-1, payload);

        String totalMessage = "";

        int cont = 0;
        for (String block : this.defragmentMessages) {
            if (block != null) {
                cont++;
            }
        }

        if (cont == Integer.parseInt(fragmentMax)){

            for (int i=0; i<Integer.parseInt(fragmentMax); i++){
                totalMessage += defragmentMessages.get(i);
            }
            defragmentMessages.clear();
            System.out.println("\n\n THIS IS IT:" +totalMessage);
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
            if (socket != null){
                socket.close();  
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

    public void listenMessage() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                String msgFromChat;

                while (socket.isConnected() && !killNode) {
                    try {
                        msgFromChat = bufferedFromTracker.readLine();
                        defragmentationFromFSTracker(msgFromChat);
                    } catch (IOException e) {
                    }
                }
            }
        }).start();
    }


    public static void main (String[] args) throws IOException{
        Socket socket = new Socket("10.0.0.10",9090); //"localhost"
        String ipNode = socket.getLocalAddress().toString().substring(1);
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



        System.out.println("Conexão FS Track Protocol com servidor " + socket.getInetAddress().getHostAddress() + " porta 9090.\n");
        Node node = new Node(ipNode,socket, ipNode + "|30|0|file1:223,3,44;file3:5,4,2;", pathToFiles);
        node.listenMessage();
        node.sendMessageToTracker();
    }

}
