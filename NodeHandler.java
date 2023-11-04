import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class NodeHandler implements Runnable {
    

    private Socket socket; // Conecção entre cliente e servidor
    private BufferedReader bufferedFromNode; // Ler informação enviada pelo cliente
    private BufferedWriter bufferedToNode; // Ler informação enviada para o cliente
    private FS_Tracker fs;
    private String ipAdress;

    public NodeHandler(Socket socket, FS_Tracker fs) throws IOException{
        this.socket = socket;
        this.bufferedToNode = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); // Enviar 
        this.bufferedFromNode = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Receber
        this.fs = fs;
        String messageReceived = bufferedFromNode.readLine();
        System.out.println(messageReceived);
        this.ipAdress = this.fs.ipAdressNode(messageReceived);
        this.fs.messageParser(messageReceived);
    }


    // Método implementado da classe FS_Tracker
    @Override
    public void run() {
        while (socket.isConnected()) {
            try{
                String aux;
                if ((aux = bufferedFromNode.readLine())==null){
                    try{
                        bufferedFromNode.close();
                        bufferedToNode.close();
                        socket.close();  
                    } catch (IOException a){
                    System.out.println("ERROR CLOSING NODE");
                    }
                    return;
                } 

                // Disconect node from FSTracker
                if (aux.charAt(0) == 'd'){
                    System.out.println("Node " + this.ipAdress + " has been disconnected");
                    this.fs.deleteDisconnectedNode(this.ipAdress);
                    try{
                        bufferedFromNode.close();
                        bufferedToNode.close();
                        socket.close();  
                    } catch (IOException a){
                    System.out.println("ERROR CLOSING NODE");
                    }
                }
                else{
                    if (aux.charAt(0) == 'i'){
                        this.fs.memoryToString();
                        System.out.println("##############");
                        this.fs.timeToString();
                        System.out.println("##############");
                        System.out.println("##############");
                        System.out.println("##############\n");
                    }
                    else if (aux.length() >= 5 && aux.substring(0, 4).equals("GET ")){
                        System.out.println("GET");
                    }
                    else{
                        //this.fs.messageParser(aux);
                        System.out.println(aux);
                        //bufferedToNode.write("SENT TO NODE"); //sendtofstracker
                        //bufferedToNode.newLine();
                        //bufferedToNode.flush();
                    }
                    //System.out.println(aux); 
                }
                
                

            } catch (IOException e){
                this.fs.deleteDisconnectedNode(ipAdress);
                try{
                    bufferedFromNode.close();
                    bufferedToNode.close();
                    socket.close();  
                } catch (IOException a){
                    System.out.println("ERROR CLOSING NODE");
                }
                break;
            }
            
        }
    }
}
