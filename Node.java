import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.*;

public class Node {
    private String ipNode;
    private List<String> files;
    private Integer size_char;
    private boolean killNode = false;

    private Socket socket;
    private BufferedReader bufferedFromTracker; // Ler informação enviada pelo servidor
    private BufferedWriter bufferedToTracker; // Ler informação enviada para o servidor


    public Node(String ip, Socket socket, String info) throws IOException{
        System.out.println("Node " + ip + " running...");
        this.ipNode = ip;
        //this.files = files;
        this.socket = socket;
        this.bufferedToTracker = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); // Enviar 
        this.bufferedFromTracker = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Receber

        sendInfoToFS_Tracker("2:3,4,5;5:2,4,2324;");
        keepAlive();
    }

    public void sendInfoToFS_Tracker(String payload) throws IOException{
        int maxPayload = 2;
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
            bufferedToTracker.write(messageToSend);
            bufferedToTracker.newLine();
            bufferedToTracker.flush();

            if (messageToSend.charAt(0) == 'd'){
                disconnectNode();
            }
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
                        System.out.println(msgFromChat);
                    } catch (IOException e) {
                    }
                }
            }
        }).start();
    }


    public String convertToString(){
        String message = "";
        for (String file : this.files){
            message += file;
            message += ";";
        }
        this.size_char = message.length();

        return message;
    }


    public static void main (String[] args) throws IOException{
        Socket socket = new Socket("localhost",9090); //"10.0.0.10"
        Node node = new Node("192.168.1.61",socket, "192.168.1.61|30|0|file1:223,3,44;file3:5,4,2;");
        node.listenMessage();
        node.sendMessageToTracker();
    }

}
