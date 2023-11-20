import java.util.Map;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

class DNS_Handler extends Thread{
    Socket socket;
    Map<String, String> list;
    BufferedWriter to;   // Escrever informação enviada para o cliente
    BufferedReader from; // Ler informação enviada pelo cliente    

    public DNS_Handler(Socket socket, Map<String, String> list) throws IOException{
        this.socket = socket;
        this.list = list;
        this.to = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        this.from = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }

    private String get_ip(String name){
        return list.get(name);
    }

    private void put_ip(String name, String ip){
        list.put(name, ip);
    }

    private String parse(String message, Integer section){
        Integer i = 1, index = 0;

        while(i < section ){
            index = message.indexOf("|", index) + 1;
            i++;
        }

        String response = "";
        if(section == 4){
            for(;index < message.length(); index++){
                response += message.charAt(index);
            }
        }else{
            for(;index < message.indexOf("|", index); index++){
                response += message.charAt(index);
            }
        }

        return response;
    }

    private void print_list(){  
        for(Map.Entry<String, String> node : list.entrySet()){
            System.out.println("Nome: " + node.getKey() + " | IP: " + node.getValue());
        }
    }

    public void run() {
        try {
            String message = from.readLine();

            // Guarda o nome do requester e ip na lista
            String requester_name = parse(message, 1);
            String ip = parse(message, 2);
            list.put(requester_name, ip);

            String type = parse(message, 3);

            if(type.equals("GET")){
                // Procura o ip do nome
                String name = parse(message, 4);

                String response = "";

                response = list.get(name);

                to.write(response);
                to.newLine();
                to.flush();
            }
        } catch (Exception e) {
            System.out.println("DNS_HANDLER ERROR: " + e.getMessage());
        }
    }
}

public class DNS {
    public static void main(String args[]) throws IOException{
        ServerSocket socketDNS = new ServerSocket(2302);
        System.out.println("SERVIÇO DNS ONLINE!");

        Map<String, String> list = new HashMap<>();

        while (!socketDNS.isClosed()) {
            new DNS_Handler(socketDNS.accept(), list).start();
        }
    }
}
