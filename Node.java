import java.util.*;

public class Node {
    private String ip;
    private List<String> files;
    private Integer size_char;


    public Node(String ip, List<String> files){
        this.ip = ip;
        this.files = files;
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

    private void sendToFS_Tracker(){
        String message = convertToString();

        String payload = this.ip + "|" + this.size_char.toString()+ "|" + message;

    }










}
