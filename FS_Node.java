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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public class FS_Node {
    private String ipNode;
    private boolean killNode = false;
    private String pathToFiles;
    private String defragmentMessages;
    private Socket socketTCP;
    private DatagramSocket socketUDP;
    private BufferedReader bufferedFromTracker; // Ler informação enviada pelo servidor
    private BufferedWriter bufferedToTracker; // Ler informação enviada para o servidor
    private ReentrantLock l = new ReentrantLock();

    // Para UDP

    private Map<String,Map<Integer,byte[]>> allNodeFiles = new HashMap<>(); // Responsável por armazenar todos os fragmentos que o nodo possui de diferentes ficheiros
    private Map<String, String> fullMessages = new HashMap<>(); //Responsável pela fragmentação se existir
    private Map<String, Integer> totalSize = new HashMap<>(); // Responsável por guardar o tamanho do último fragmento a ser enviado em bytes
    private Map<String, Integer> fragmentoAtual = new HashMap<>(); // Responsável por guardar o fragmento atual que um determinado download está
    private Map<String, Integer> n_sequencia_esperado = new HashMap<>(); // Responsável por guardar o número de sequencia esperado de um determinado download
    private Map<String, String> ipToSendAKCS = new HashMap<>(); // Responsável por identificar o ip do nodo para onde temos de enviar as mensagens de confirmação de rececção de um pacote
    private Map<String, Long> rttTimes = new HashMap<>(); // Responsável por guardar os RTTs dos vários nodos
    private Map<String, LocalTime> desconexoes = new HashMap<>(); // Responsável por tratar dos nodos que estão ativos para download


    private Set<String> allIpsToSend = new HashSet<>();
    private Set<String> preventDups = new HashSet<>();
    private String fileName;
    private boolean hasDownloadStarted = false;
    private boolean needToDownloadAgain = false;
    private List<String> hasStarted = new ArrayList<>(); // Responsável por identificar todos os downloads que já começaram


    public FS_Node(String ip, Socket socketTCP, String pathToFiles, DatagramSocket socketUDP) throws IOException{
        this.ipNode = ip;
        this.defragmentMessages = "";
        this.pathToFiles = pathToFiles;
        this.socketUDP = socketUDP;
        this.socketTCP = socketTCP;
        this.bufferedToTracker = new BufferedWriter(new OutputStreamWriter(socketTCP.getOutputStream())); // Enviar 
        this.bufferedFromTracker = new BufferedReader(new InputStreamReader(socketTCP.getInputStream())); // Receber
        keepAlive();
        String msg = getFilesInfo();
        if (msg != ""){
            sendInfoToFS_Tracker(msg);
        }
        System.out.println("Ready to interact with network...");
    }




    ////////////////////////////////////////////////////
    ////////////////////////////////////////////////////
    //COMUNICAÇÃO TCP
    ////////////////////////////////////////////////////
    ////////////////////////////////////////////////////



    // Vai a cada ficheiro e guarda-o numa hashmap e cria uma mensagem para ser enviada ao FS_Tracker indicando
    // que blocos é que cada ficheiro possui
    public String getFilesInfo(){
        String payload = "";
        File infoFile = new File(this.pathToFiles);
        File[] allFiles = infoFile.listFiles();

        for (File file : allFiles){
            String fileName = file.getName();
            
            Map<Integer, byte[]> fragmentFile = new HashMap<>();

            Path path = file.toPath();
            byte[] fileInBytes = null;

            try {
                fileInBytes = Files.readAllBytes(path);
            } catch (IOException e) {
                e.printStackTrace();
            }  



            int maxPacketSize = 1017;
            int totalBytes = fileInBytes.length;
            int totalFragments = (int)Math.ceil((double)totalBytes/maxPacketSize);
            payload += fileName + ":1-"+totalFragments+";";
            for (int i = 1; i <= totalFragments; i++){
                int start = (i - 1) * maxPacketSize;
                int end = i * maxPacketSize;
                
                if (end >= totalBytes) {
                    end = totalBytes;
                }
                
                fragmentFile.put(i, Arrays.copyOfRange(fileInBytes, start, end));
            }
            allNodeFiles.put(fileName, fragmentFile);
        }
        return payload;                
    }


    // Envio de informação para o FS_Tracker com fragmentação de pacotes, se necessário
    public void sendInfoToFS_Tracker(String payload) throws IOException{
        int maxPayload = 1024;
        int payloadSize = payload.length();
        
        // Mensagem não fragmentada
        if (payloadSize<=maxPayload) {
            String finalMessage = this.ipNode + "|" + 1 + "|" + payload;
            // System.out.print("Payload Sent to Tracker: " + finalMessage + "\n\n");  // COLOCAR ATIVO PARA DEMONSTRAR
            bufferedToTracker.write(finalMessage);
            bufferedToTracker.newLine();
            bufferedToTracker.flush();
        }
        // Mensagem fragmentada
        else{
            int totalFragments = (int)Math.ceil((double)payloadSize/maxPayload);

            for (int i = 1; i <= totalFragments; i++){
                int start = (i * maxPayload) - maxPayload;
                int end = i * maxPayload;
                String message = "";
                if (end >= payloadSize) {
                    end = payloadSize;
                    // Mensagem com fragmentação (É a última)
                    message = this.ipNode + "|" + 3 + "|" + payload.substring(start, end);
                }
                else{
                    // Mensagem com fragmentação (não é a última)
                    message = this.ipNode + "|" + 2 + "|" + payload.substring(start, end);           
                }
                // System.out.println("Payload Sent to Tracker: " + message);  // COLOCAR ATIVO PARA DEMONSTRAR
                bufferedToTracker.write(message);
                bufferedToTracker.newLine();
                bufferedToTracker.flush();
            }
      
        }
    }


    // Responsável por verificar se o nodo nunca perde a conexão com o FS_Tracker
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

            timer.scheduleAtFixedRate(task, 1000, 1000);
        }).start();
    }


    // Trata de enviar determinadas mensagens com vários significados e funcionalidades ao FS_Tracker
    public void sendMessageToTracker() throws IOException {
        new Thread(() -> {

            Scanner scanner = new Scanner(System.in);
            while(socketTCP.isConnected()  && !killNode) {
                String messageToSend = scanner.nextLine();

                // Pedido de desconexão
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
                // Pedido de consulta de ficheiros e blocos
                else if(messageToSend.equals("i")){
                    try{
                        bufferedToTracker.write(messageToSend);
                        bufferedToTracker.newLine();
                        bufferedToTracker.flush();
                    }
                    catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                }
                // Pedido de download de ficheiro
                else if(messageToSend.startsWith("GET ")){
                    try{
                        downloadStops();
                        fileName = messageToSend.substring(4);
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


    // Função responsável por ouvir sempre se existe infomação a ser enviada para esta porta
    public void listenMessageFromTracker() {
        new Thread(() -> {
            String msgFromChat;

            while (socketTCP.isConnected() && !killNode) {
                try {
                    msgFromChat = bufferedFromTracker.readLine();
                    defragmentationFromFSTracker(msgFromChat);
                } catch (IOException e) { }
            }
        }).start();
    }


    // Responsável por defragmentar informações enviadas pelo FS_Tracker
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


    // Função responsável por receber payload que indica que blocos são necessários de ir buscar a para um determinado Node
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
        
        sendToNodes(blocksToRetreive);
    }


    // Responsável por fechar comunicações entre o Node e outros Nodes e FS_Tracker
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





    public Map<Integer, String> escolherIP(Map<String, Integer> sortedMap, Map<Integer, List<String>> blocksToRetreive){
        Map<Integer, String> transfers = new HashMap<>();
        Map<String, Long> rttTime = this.rttTimes;
        Map<String, Integer> map = sortedMap;
        String ip = "";
        Integer comparator = -1;
        Integer temp = -1;
        for (Map.Entry<Integer, List<String>> entry : blocksToRetreive.entrySet()) {
            ip = "";
            comparator = -1;
            temp = -1;
            Integer bloco = entry.getKey();
            for(String ipAux : entry.getValue()){

                if (map.containsKey(ipAux)){
                    temp = map.get(ipAux);
                    if(comparator == -1){
                        comparator = map.get(ipAux);
                        ip = ipAux;
                    }
                    if(temp < comparator){
                        comparator = map.get(ipAux);
                        ip = ipAux;
                    }
                    else if(temp == comparator){
                        int numTransIp = 0;
                        int numTransIpAux = 0;
                        for (Map.Entry<Integer, String> entry2 : transfers.entrySet()) {
                            if(entry2.getValue().equals(ipAux)){
                                numTransIp++;
                            }
                            if(entry2.getValue().equals(ip)){
                                numTransIpAux++;
                            }
                        }
                        if(numTransIpAux > numTransIp){
                            comparator = map.get(ipAux);
                            ip = ipAux;
                        }else if(numTransIp >= numTransIpAux){
                            comparator = map.get(ip);
                        }
                    }
                }
            }
            transfers.put(bloco, ip);
            long rtt = rttTime.get(ip);
            int rttTimeDoIp = ((int) rtt)/2;
            int c = map.get(ip);
            map.put(ip, c+rttTimeDoIp);
            rttTimeDoIp = 0;
            c = 0;
        }
        return transfers;
    }


    public void getAllRTT (Set<String> allIps){
        System.out.println("\n-----------------");
        for (String ip : allIps){  
            while(true){
                if(!rttTimes.containsKey(ip)){
                    try {
                        sendMessageToNode("Q|"+ipNode+"|"+System.currentTimeMillis(), ip);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    break;
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("RTT time de "+ip +":  "+ rttTimes.get(ip));
        }
        System.out.println("-----------------\n");
    }


    // Algoritmo que determina que blocos são necessários ir buscar a cada Node
    public void sendToNodes (Map<Integer, List<String>> blocksToRetreive){
        Map<String, Integer> numDeAparicoes = new HashMap<>();
        Iterator<Map.Entry<Integer, List<String>>> iterator = blocksToRetreive.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, List<String>> entry = iterator.next();

            if (!entry.getValue().contains(ipNode)) {
                for (String ip : entry.getValue()) {
                    if (!numDeAparicoes.containsKey(ip)) {
                        numDeAparicoes.put(ip, 1);
                    } else {
                        numDeAparicoes.put(ip, numDeAparicoes.get(ip) + 1);
                    }
                }
            } else {
                iterator.remove();
            }
    
        }

        // Ordenar por ordem decrescente de número de aparições
        List<Map.Entry<String, Integer>> list = new ArrayList<>(numDeAparicoes.entrySet());
        list.sort(Map.Entry.comparingByValue());
        Collections.reverse(list);
        
        // Voltar a colocar numa hashmap
        Map<String, Integer> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
            allIpsToSend.add(entry.getKey());
        }

        getAllRTT(allIpsToSend);

        for (Map.Entry<String,Integer> entry : sortedMap.entrySet()) {
            String ip = entry.getKey();
            int rtt = (Math.toIntExact(this.rttTimes.get(ip)))/2;
            int c = entry.getValue()/2;
            sortedMap.put(ip, c+rtt);
        }


        Map<Integer, String> transfers = escolherIP(sortedMap, blocksToRetreive);
        Map<String, String> ipEBlocos = new HashMap<>();

        for(Map.Entry<Integer, String> entry : transfers.entrySet()){
            Integer numbloco = entry.getKey();
            String ip = entry.getValue();
            if (!ipEBlocos.containsKey(ip)){
                ipEBlocos.put(ip, numbloco.toString());
            }
            else{
                String aux = ipEBlocos.get(ip)+","+numbloco.toString();
                ipEBlocos.put(ip, aux);
            }
        }

        if (ipEBlocos.isEmpty()) System.out.println("You already have that file!");

        for (Map.Entry<String, String> entry : ipEBlocos.entrySet()) {
            new Thread(() -> {
                String ipParaPedirBlocos = entry.getKey();
                String blocosPedidosAoIP = entry.getValue();
                String message = "0|"+ipNode+"|"+fileName+"|"+blocosPedidosAoIP;
                if (message.length()<=1024){
                    try {
                        sendMessageToNode(message, ipParaPedirBlocos);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    while (true) {
                        try {
                            long rtt = rttTimes.get(ipParaPedirBlocos);
                            Thread.sleep(rtt);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        l.lock();
                        try{
                            if(!hasStarted.contains(ipParaPedirBlocos)){
                                try {
                                    sendMessageToNode(message, ipParaPedirBlocos);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                //System.out.println("Resending Asking For file");
                            }
                            else{
                                break;
                            }
                        }
                        finally{
                            l.unlock();
                        }
                        
                    }
                    l.lock();
                    try{
                        hasStarted.remove(ipParaPedirBlocos);    
                    } 
                    finally{
                        l.unlock();
                    }
                }
                else{
                    fragmentToUDPIfNeeded(message, ipParaPedirBlocos);
                }
                
            }).start();
            
        }
    }

    // Fragmentar as mensagens para um determinado nodo
    public void fragmentToUDPIfNeeded(String messageToSend, String ipToSend){
        int tamanhoTotalMensagem = messageToSend.length();
        String[] split = messageToSend.split("\\|");
        String payload = split[3];
        int cont = 0;
        int index = 0;
        String fileBlockName = "i"+ipNode+payload.substring(0,payload.indexOf(","));
        l.lock();
        try{
            fragmentoAtual.remove(fileBlockName);    
        }
        finally{
            l.unlock();
        }
        

        for (int i = 0; i < messageToSend.length(); i++) {
            if (messageToSend.charAt(i) == '|') {
                cont++;
                if (cont == 3) {
                    index = i;
                    break;
                }
            }
        }

        String cabecalho = messageToSend.substring(0, index + 1);
        //System.out.println(cabecalho);
        int tamanhoCabecalho = cabecalho.length();
        String blocoInicial= payload.substring(0,payload.indexOf(","));
        int max_payload_size = 1024 - tamanhoCabecalho - 10 - blocoInicial.length();

        // Quantos fragmentos sao necessarios para enviar toda a informacao em pacotes de 1024 bytes
        int numFragmentos = (int) Math.ceil((double) tamanhoTotalMensagem / max_payload_size);

        cabecalho = '1' + cabecalho.substring(1);

        for (int i = 1; i <= numFragmentos; i++) {
            int start = (i - 1) * max_payload_size;
            int end = i * max_payload_size;
            if (end >= payload.length()) {
                end = payload.length();
                i = numFragmentos;
            }
            if (i == numFragmentos) {
                cabecalho = '2' + cabecalho.substring(1);
            }
            try {
                sendMessageToNode(cabecalho + i + "|" + blocoInicial + "|" + payload.substring(start, end), ipToSend);
                //System.out.println("ACK -> "+ (i)); // NEEDED FOR DEBBUG
            } catch (IOException e) {
                 e.printStackTrace();
            }   


            int keepCheck=0;
            int fragNow;
            while (true) {

                try {
                    long rtt = rttTimes.get(ipToSend);
                    Thread.sleep(rtt);
                    // System.out.println(rtt); // NEEDED FOR DEBBUG
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                l.lock();                      
                try{ 
                    fragNow = fragmentoAtual.getOrDefault(fileBlockName, -1);
                }
                finally{
                    l.unlock();
                }

                if((i+1)!=fragNow){
                    if(keepCheck==150) return;
                    keepCheck++;
                    try {
                        sendMessageToNode(cabecalho + i + "|" + blocoInicial + "|" + payload.substring(start, end), ipToSend);
                        //System.out.println("Resending ..... ACK -> "+ (i-1));  // NEEDED FOR DEBBUG
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    break;
                }
            }
        }
    }



    // Recebe um payload e determina que blocos são necessários
    public void separateEachFile (String responseFromNode){
        String[] subst = responseFromNode.split("\\|");
        String ipDestino = subst[1];
        String filename = subst[2];
        String payload = subst[3];
        String[] substrings = payload.split("\\,");
        List<Integer> blocos = new ArrayList<>();
        for(String info: substrings){
            blocos.add(Integer.parseInt(info));
        }
        sendFiles(filename,ipDestino, blocos);
    }


    // Enviar fragmentos ao nodo que os pediu
    // Multithread para funcionar em paralelo
    public void sendFiles(String filename, String ipToSend, List<Integer> blocos){
        if (preventDups.contains(filename+ipToSend)) return;
        preventDups.add(filename+ipToSend);
        new Thread(() -> {
            System.out.println("Número de blocos a enviar para "+ipToSend+" = "+blocos.size());
            Collections.sort(blocos);
            String fileBlockName;
            int lastElement;
            int lengthLast;
            l.lock();
            try{
                fileBlockName=ipToSend+blocos.get(0);
                n_sequencia_esperado.remove("i+"+fileBlockName);
                lastElement = blocos.get(blocos.size()-1);
                lengthLast = allNodeFiles.get(filename).get(lastElement).length;
                fragmentoAtual.remove(fileBlockName);
                try {
                    sendMessageToNode("F|"+blocos.get(0)+"|"+lengthLast+"|"+this.ipNode, ipToSend);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            finally{
                l.unlock();
            }

            while (true) {

                try {
                    long rtt = rttTimes.get(ipToSend);
                    Thread.sleep(rtt);
                    // System.out.println(rtt); // NEEDED FOR DEBBUG
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                l.lock();
                try{
                    if(!fragmentoAtual.containsKey(fileBlockName)){
                        try {
                            sendMessageToNode("F|"+blocos.get(0)+"|"+lengthLast+"|"+this.ipNode, ipToSend);
                            //System.out.println("Resending ..... F "); // NEEDED FOR DEBBUG
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    else{
                        break;
                    }
                }
                finally{
                    l.unlock();
                }

            }
            boolean isLastFragment = false;
            int i = 0;
            for (Integer blocknum : blocos){
                i++;
                int keepCheck=0;

                // Mensagem com fragmentação (É a última)
                if (blocknum==lastElement) {
                    isLastFragment = true;
                }
                byte[] eachMessage = new byte[1024];
                if (isLastFragment){
                    System.out.println("Acabou envio para "+ipToSend+"\n\n");
                    eachMessage[0] = (byte) (1);
                }
                else{
                    eachMessage[0] = (byte) (0);
                }
                eachMessage[1] = (byte) (i & 0xFF);
                eachMessage[2] = (byte) ((i >> 8) & 0xFF);
                eachMessage[3] = (byte) (blocos.get(0) & 0xFF);
                eachMessage[4] = (byte) ((blocos.get(0) >> 8) & 0xFF);
                eachMessage[5] = (byte) (blocknum & 0xFF);
                eachMessage[6] = (byte) ((blocknum >> 8) & 0xFF);

                byte[] file = null;
                l.lock();
                try{
                    file = allNodeFiles.get(filename).get(blocknum);
                }
                finally{
                    l.unlock();
                }



                System.arraycopy(file, 0, eachMessage, 7, file.length);
                try {
                    sendMessageToNodeInBytes(eachMessage,ipToSend);
                    //System.out.println("ACK -> "+ i); // NEEDED FOR DEBBUG
                } catch (IOException e) {
                    e.printStackTrace();
                }

                int fragNow;
                while (true) {
                    try {
                        long rtt = rttTimes.get(ipToSend);
                        Thread.sleep(rtt);
                        //System.out.println(rtt);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                    l.lock();                      
                    try{
                        fragNow = fragmentoAtual.get(fileBlockName);
                    }
                    finally{
                        l.unlock();
                    }


                    if(i+1!=fragNow){
                        if(keepCheck==150){
                            preventDups.remove(filename+ipToSend);
                            return;
                        } 

                        keepCheck++;
                        try {
                            sendMessageToNodeInBytes(eachMessage,ipToSend);
                            //System.out.println("Resending ..... ACK -> "+ i);  // NEEDED FOR DEBBUG
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    else{
                        break;
                    }
                }
            }
            preventDups.remove(filename+ipToSend);
        }).start();
    }


    // Receber pacotes fragmentados e fazer a verificação se o pacote que recebemos é o correto ou não
    // Enviar ACKs de resposta
    public void getFragmentedUDP(String payload){
        String[] split = payload.split("\\|");

        int lastFragment = Integer.parseInt(split[0]);
        String ipToSendACK = split[1];
        String name = split[2];
        int numero_sequencia = Integer.parseInt(split[3]);
        int blocoIn = Integer.parseInt(split[4]);
        String nameFile = "i"+ipToSendACK+blocoIn;
        String mensagem = split[5];
        int n_seq_esperado;

        l.lock();
        try{
            n_seq_esperado = n_sequencia_esperado.getOrDefault(nameFile, 1);
        }
        finally{
            l.unlock();
        }

        if (numero_sequencia == n_seq_esperado){

            try{
                if (lastFragment == 2){

                    l.lock();
                    try{
                        String msg=fullMessages.get(nameFile);
                        msg+=mensagem;
                        separateEachFile("0|"+ipToSendACK+"|"+name+"|"+msg);
                        sendMessageToNode("ACK"+n_seq_esperado+"|"+nameFile, ipToSendACK);
                        //System.out.println("ACK"+(n_seq_esperado)+"|"+nameFile); // DELETE AFTER
                    }
                    finally{
                        l.unlock();
                    }
                    
                }
                else{

                    l.lock();
                    try{
                        String msg ="";
                        if (numero_sequencia != 1) msg=fullMessages.get(nameFile);
                        msg+=mensagem;
                        fullMessages.put(nameFile, msg);
                        sendMessageToNode("ACK"+n_seq_esperado+"|"+nameFile, ipToSendACK);
                        //System.out.println("ACK"+(n_seq_esperado)+"|"+nameFile); // DELETE AFTER
                    }
                    finally{
                        l.unlock();
                    }

                }

                l.lock();
                try{
                    n_sequencia_esperado.put(nameFile, n_seq_esperado+1);    
                }
                finally{
                    l.unlock();
                }
                
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
        else{
            l.lock();
            try{
                try{
                    sendMessageToNode("ACK"+(n_seq_esperado-1)+"|"+nameFile, ipToSendACK);
                    //System.out.println("ACK"+(n_seq_esperado)+"|"+nameFile); // DELETE AFTER
                } catch (IOException e){ }
            }
            finally{
                l.unlock();
            }
        }


    }









    // Receber pacotes e fazer a verificação se o pacote que recebemos é o correto ou não
    // Enviar ACKs de resposta
    public void getFile(byte[] messageFragment){
        int last_fragment = (messageFragment[0]);
        int numero_sequencia = ((messageFragment[1] & 0xFF) | ((messageFragment[2] << 8) & 0xFF00));
        int firstBlock = ((messageFragment[3] & 0xFF) | ((messageFragment[4] << 8) & 0xFF00));
        int blocknumber = ((messageFragment[5] & 0xFF) | ((messageFragment[6] << 8) & 0xFF00));
        String nameFile=ipNode+firstBlock;
        int n_seq_esperado;
        byte[] file_info=null;
        l.lock();
        try{
            if (desconexoes.containsKey(nameFile)) desconexoes.put(nameFile, LocalTime.now());
            n_seq_esperado = n_sequencia_esperado.get(nameFile);
        }
        finally{
            l.unlock();
        }


        // Se o número de sequencia for o esperado, tudo corre bem
        if (numero_sequencia == n_seq_esperado){

            try{
                if (last_fragment == 1){
                    int size;
                    l.lock();
                    try{
                        size = totalSize.get(nameFile);
                    }
                    finally{
                        l.unlock();
                    }
                    

                    file_info = new byte[size];
                    System.arraycopy(messageFragment, 7, file_info, 0, size);

                    l.lock();
                    try{
                        sendMessageToNode("ACK"+n_seq_esperado+"|"+nameFile, ipToSendAKCS.get(nameFile));
                        desconexoes.remove(nameFile);
                    }
                    finally{
                        l.unlock();
                    }
                    
                }
                else{
                    file_info = new byte[1017];
                    System.arraycopy(messageFragment, 7, file_info, 0, file_info.length);

                    l.lock();
                    try{
                        sendMessageToNode("ACK"+n_seq_esperado+"|"+nameFile, ipToSendAKCS.get(nameFile));
                    }
                    finally{
                        l.unlock();
                    }

                }

                l.lock();
                try{
                    n_sequencia_esperado.put(nameFile, n_seq_esperado+1);    
                }
                finally{
                    l.unlock();
                }
                
            }
            catch (IOException e){
                e.printStackTrace();
            }
            try {
                sendInfoToFS_Tracker(fileName+":"+blocknumber+";");

                l.lock();
                try{
                    if (!allNodeFiles.containsKey(fileName)) {
                        allNodeFiles.put(fileName, new HashMap<>());
                    }
                    Map<Integer, byte[]> innerMap = allNodeFiles.get(fileName);
                    innerMap.put(blocknumber, file_info);
                    allNodeFiles.put(fileName, innerMap);
                }
                finally{
                    l.unlock();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        else{
            l.lock();
            try{
                try{
                    sendMessageToNode("ACK"+(n_seq_esperado-1)+"|"+nameFile, ipToSendAKCS.get(nameFile));
                } catch (IOException e){ }
            }
            finally{
                l.unlock();
            }

        }

    }

    


    // Ouvir mensagens de outros nodes
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

                        // Primeira mensagem
                        if (responsenFromNode.startsWith("0|")){
                            //System.out.println("Received: " + responsenFromNode.substring(0,30)); // DELETE AFTER
                            separateEachFile(responsenFromNode);
                        }

                        // Mensagem fragmentada
                        else if (responsenFromNode.startsWith("1|") || responsenFromNode.startsWith("2|")){
                            //System.out.println("Received: " + responsenFromNode.substring(0,30)); // DELETE AFTER
                            getFragmentedUDP(responsenFromNode);
                        }

                        // Primeira mensagem que o nodo que pediu download a outro nodo recebe
                        else if (responsenFromNode.startsWith("F|")){
                            String[] split = responsenFromNode.split("\\|");
                            String filename = ipNode+split[1];
                            //System.out.println("Received FileName: " + filename); // NEEDED FOR DEBBUG
                            l.lock();
                            try{
                                desconexoes.put(filename, LocalTime.now());
                                n_sequencia_esperado.put(filename, 1);
                                hasStarted.add(split[3]);
                                totalSize.put(filename,  Integer.parseInt(split[2]));
                                ipToSendAKCS.put(filename, split[3]); // fileName -> ipOrigem 
                            }
                            finally{
                                l.unlock();
                            }
                            hasDownloadStarted = true;
                            sendMessageToNode("ACK0|"+filename, split[3]);
                        }

                        // Validação dos ACKs recebidos
                        else if (responsenFromNode.startsWith("ACK")){
                            String[] split = responsenFromNode.split("\\|");
                            int ack_num = Integer.parseInt(split[0].substring(3));
                            String fName= split[1];

                            //if (fName.charAt(0) == 'i') desconexoes.put(fName.substring(1), LocalTime.now());

                            l.lock();
                            try{
                                fragmentoAtual.put(fName, ack_num+1);
                            }
                            finally{
                                l.unlock();
                            }
                        }

                        else if (responsenFromNode.startsWith("Q")){
                            String[] split = responsenFromNode.split("\\|");
                            String ip = split[1];
                            long endtime = System.currentTimeMillis();
                            long timestamp = Long.parseLong(split[2]);
                            long rtt = (long) ((endtime - timestamp)*2.2);

                            l.lock();
                            try{
                                if (rtt < 2) rtt=2;
                                if(!rttTimes.containsKey(ip)){
                                    rttTimes.put(ip, rtt);  
                                    System.out.println("RTT calculated ---- "+ip+" -->"+rttTimes.get(ip)); 
                                } 
                            }
                            finally{
                                l.unlock();
                            }

                           sendMessageToNode("A|"+ipNode+"|"+rtt, ip);
                        }

                        else if (responsenFromNode.startsWith("A")){
                            String[] split = responsenFromNode.split("\\|");
                            String ip = split[1];
                            long rtt = Long.parseLong(split[2]);

                            l.lock();
                            try{
                                if(!rttTimes.containsKey(ip)) rttTimes.put(ip, rtt);  
                            }
                            finally{
                                l.unlock();
                            }
                        }

                        // Receber os blocos relativos a um determinado ficheiro
                        else{
                            getFile(receivePacket.getData());
                        }
                    }
                    catch (Exception e) {
                    }
                }
            }
        }).start();
    }

    // Enviar uma mensagem em String para um nodo
    public void sendMessageToNode(String messageToSend, String ipToSend) throws IOException {
        l.lock();
        try{
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] buf = messageToSend.getBytes();
            DatagramPacket p = new DatagramPacket(buf, buf.length, InetAddress.getByName(ipToSend), 9090);
            clientSocket.send(p);
            clientSocket.close();
            //System.out.println("Sent: "+messageToSend); // NEEDED FOR DEBBUG
        }
        finally{
            l.unlock();
        }
    }



    // Enviar uma mensagem em bytes para um nodo
    public void sendMessageToNodeInBytes(byte[] messageToSend, String ipToSend) throws IOException {
        l.lock();
        try{
            DatagramSocket clientSocket = new DatagramSocket();
            DatagramPacket p = new DatagramPacket(messageToSend, messageToSend.length, InetAddress.getByName(ipToSend), 9090);
            clientSocket.send(p);
            clientSocket.close();
        }
        finally{
            l.unlock();
        }

    }


    // Timer responsável por verificar se algum nodo se desconecta a meio de um download
    private Timer timer2;
    public void downloadStops(){
        new Thread(() -> {
            timer2 = new Timer();

            TimerTask task = new TimerTask() {
                
                @Override
                public void run() {

                    if(!killNode && !desconexoes.isEmpty()){
                        LocalTime now = LocalTime.now();
                        Iterator<Map.Entry<String, LocalTime>> iterator = desconexoes.entrySet().iterator();

                        while (iterator.hasNext()) {
                            Map.Entry<String, LocalTime> entry = iterator.next();
                            LocalTime before = entry.getValue();
                            Duration duration = Duration.between(before, now);
                            long secondsDifference = duration.getSeconds();

                            if (secondsDifference >= 3) {
                                iterator.remove();
                                desconexoes.remove(entry.getKey());
                                needToDownloadAgain=true;
                                System.out.println("DESCONEXAO"+entry.getKey());
                            }
                        }
                    }
                                

                    if(hasDownloadStarted && desconexoes.isEmpty()){
                        hasDownloadStarted = false;
                        if(needToDownloadAgain) {
                            desconexoes.clear();
                            needToDownloadAgain=false;
                            try {
                                bufferedToTracker.write("GET "+fileName);
                                bufferedToTracker.newLine();
                                bufferedToTracker.flush();
                                return;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        System.out.println("Download completed!");
                        int len = 0;
                        Map<Integer, byte[]> outputBlocks = allNodeFiles.get(fileName);
                        for (Map.Entry<Integer, byte[]> entry : outputBlocks.entrySet()) {
                            len+=entry.getValue().length;
                        }
                        int currentIndex = 0;
                        byte[] result = new byte[len];
                        for (byte[] byteArray : outputBlocks.values()) {
                            System.arraycopy(byteArray, 0, result, currentIndex, byteArray.length);
                            currentIndex += byteArray.length;
                        }

                        Path directoryPath = Paths.get(pathToFiles);
                        Path filePath = directoryPath.resolve(fileName);

                        // Escreve o conteúdo no arquivo
                        try {
                            Files.write(filePath, result);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        
                        timer2.cancel();
                        timer2.purge();
                    }
                }
            };

            timer2.scheduleAtFixedRate(task, 500, 500);
        }).start();
    }





























    public static void main (String[] args) throws IOException{

        int numeroMaximoDeThreads = 20;
        // Configurar o número máximo de threads em paralelo
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(numeroMaximoDeThreads));


        InetAddress address = InetAddress.getByName("fstracker.cc.com");
        String trackerIP = address.getHostAddress();
        Socket socketTCP = new Socket(trackerIP, 9090); //"localhost"
        String ipNode = socketTCP.getLocalAddress().toString().substring(1);

        DatagramSocket socketUDP = new DatagramSocket(9090);

        String pathToFiles;
        if (ipNode.equals("132.50.1.20")){
            pathToFiles = "/home/core/Desktop/Projeto/Node1";
        }
        else if (ipNode.equals("192.168.2.20")){
            pathToFiles = "/home/core/Desktop/Projeto/Node2";
        }
        else if (ipNode.equals("192.168.3.20")){
            pathToFiles = "/home/core/Desktop/Projeto/Node3";
        }
        else{
            pathToFiles = "/home/core/Desktop/Projeto/Node4";
        }


        System.out.println("Conexão FS Track Protocol com servidor " + socketTCP.getInetAddress().getHostAddress() + " porta 9090.");
        System.out.println("FS Transfer Protocol: à escuta na porta 9090.\n");
        FS_Node node = new FS_Node(ipNode, socketTCP, pathToFiles, socketUDP);
        node.listenMessageFromTracker();
        node.sendMessageToTracker();
        
        node.listenMessageFromNode();
    
        
    }

}




