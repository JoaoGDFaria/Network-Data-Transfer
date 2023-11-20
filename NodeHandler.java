import java.io.*;
import java.net.*;

/**
 * CLASS HANDLER - Trata de pedidos de um cliente
 */
class NodeHandler extends Thread{
    private Socket socket;                   // Conecção entre cliente e servidor
    private BufferedWriter bufferedToNode;   // Escrever informação enviada para o cliente
    private BufferedReader bufferedFromNode; // Ler informação enviada pelo cliente
    private FS_Tracker tracker;
    private String ipAddress;

    public NodeHandler(Socket socket, FS_Tracker tracker) throws IOException{
        this.socket = socket;
        this.bufferedToNode = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); // Enviar 
        this.bufferedFromNode = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Receber
        this.tracker = tracker;
        String messageReceived = bufferedFromNode.readLine();
        this.ipAddress = tracker.ipAdressNode(messageReceived);

        System.out.println("Node " + ipAddress + " is connected.");
        //System.out.println(messageReceived);  // COLOCAR ATIVO PARA DEMONSTRAR
        tracker.messageParser(messageReceived);
    }

    public void run(){
        try{
            while(socket.isConnected()){
                String req = bufferedFromNode.readLine();

                if(req == null){
                    bufferedFromNode.close();
                    bufferedToNode.close();
                    socket.close();
                    break;
                }

                // Disconnect node from FSTracker
                if (req.charAt(0) == 'd'){
                    this.tracker.deleteDisconnectedNode(this.ipAddress);
                    break;
                }
                else{
                    if (req.charAt(0) == 'i'){
                        this.tracker.memoryToString();
                        System.out.println("##############");
                        this.tracker.timeToString();
                        System.out.println("##############");
                        System.out.println("##############");
                        System.out.println("##############\n");
                    }else if (req.length() >= 5 && req.startsWith("GET ")){
                        String messageToNode = this.tracker.pickFile(req.substring(4), bufferedToNode);
                        this.tracker.sendInfoToNode(messageToNode,bufferedToNode);
                    }else{
                        this.tracker.messageParser(req);
                        //System.out.println(req);  // COLOCAR ATIVO PARA DEMONSTRAR
                    }
                    //System.out.println(req); 
                }

            }
        }catch(IOException e){
            System.out.println("ERRO HANDLER: " + e.getMessage());
        }
    }
}