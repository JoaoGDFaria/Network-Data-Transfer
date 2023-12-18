import java.io.*;
import java.net.*;

/**
 * CLASS HANDLER - Trata de pedidos de um cliente específico
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
        this.ipAddress = tracker.ipAddressNode(messageReceived);

        System.out.println("Node " + ipAddress + " is connected.");
        tracker.messageParser(messageReceived);
    }


    public void run(){
        try{
            while(socket.isConnected()){
                String req = bufferedFromNode.readLine();

                // Se por algum motivo os buffers não estiverem ativos fechar as sockets
                if(req == null){
                    bufferedFromNode.close();
                    bufferedToNode.close();
                    socket.close();
                    break;
                }

                // Disconectar o nodo manualmente do FSTracker
                if (req.charAt(0) == 'd'){
                    this.tracker.deleteDisconnectedNode(this.ipAddress);
                    bufferedFromNode.close();
                    bufferedToNode.close();
                    socket.close();
                    break;
                }
                else{
                    // Pedir ao FS_Tracker que mostre todos os ficheiros e blocos que possui assim como os respetivos ips
                    if (req.charAt(0) == 'i'){
                        this.tracker.memoryToString();
                        System.out.println("##############");
                        this.tracker.timeToString();
                        System.out.println("##############");
                        System.out.println("##############");
                        System.out.println("##############\n");
                    }
                    // Pedir ao FS_Tracker que envie para o nodo informação relativa a um ficheiro em concreto (caso exista)
                    else if (req.length() >= 5 && req.startsWith("GET ")){
                        String messageToNode = this.tracker.pickFile(req.substring(4), bufferedToNode);
                        this.tracker.sendInfoToNode(messageToNode,bufferedToNode);
                    }
                    // Caso nenhum acima se aplique tratar cada mensagem dentro da função messageParser
                    else{
                        this.tracker.messageParser(req);
                    }
                }
            }
        }catch(IOException e){
            System.out.println("ERRO HANDLER: " + e.getMessage());
        }
    }
}