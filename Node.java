import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Node {
    private String ipNode;
    private List<String> files;
    private Integer size_char;

    private Socket socket;
    private BufferedReader bufferedFromTracker; // Ler informação enviada pelo servidor
    private BufferedWriter bufferedToTracker; // Ler informação enviada para o servidor


    public Node(String ip, Socket socket, String info) throws IOException{
        this.ipNode = ip;
        //this.files = files;
        this.socket = socket;
        this.bufferedToTracker = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); // Enviar 
        this.bufferedFromTracker = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Receber

        bufferedToTracker.write("MEUIP|2|file1:223,3,44;file3:5,4,2;");
        bufferedToTracker.newLine();
        bufferedToTracker.flush();
    }



    public void sendMessageToTracker() throws IOException {

        Scanner scanner = new Scanner(System.in);
        while(socket.isConnected()) {
            String messageToSend = scanner.nextLine();
            bufferedToTracker.write(messageToSend);
            bufferedToTracker.newLine();
            bufferedToTracker.flush();
        }
    }


    public void listenMessage() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                String msgFromChat;

                while (socket.isConnected()) {
                    try {
                        msgFromChat = bufferedFromTracker.readLine();
                        System.out.println(msgFromChat);
                    } catch (IOException e) {
                        e.printStackTrace();
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

    private String sendToFS_Tracker(){
        String message = convertToString();

        String payload = this.ipNode + "|" + this.size_char.toString()+ "|" + message;

        return payload;
    }





    public static void main (String[] args) throws IOException{
        Socket socket = new Socket("localhost",1234);
        Node node = new Node("IP",socket, "MEUIP|2|file1:223,3,44;file3:5,4,2;");
        node.listenMessage();
        node.sendMessageToTracker();
    }

}
