import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;


public class Main {
    public static void main(String[] args){

        FS_Tracker fs = new FS_Tracker();
        // File A
        fs.insertInfo("A", 1, "192.168.1.1");
        fs.insertInfo("A", 2, "192.168.1.6");
        fs.insertInfo("A", 3, "192.168.1.3");
        fs.insertInfo("A", 4, "192.168.1.4");
        fs.insertInfo("A", 1, "192.168.1.4");
        fs.insertInfo("A", 2, "192.168.1.1");
        fs.insertInfo("A", 4, "192.168.1.6");
        fs.insertInfo("A", 3, "192.168.1.2");
        fs.insertInfo("A", 1, "192.168.1.4");

        // File B
        fs.insertInfo("B", 1, "192.168.1.5");
        fs.insertInfo("B", 2, "192.168.1.3");
        fs.insertInfo("B", 3, "192.168.1.7");
        fs.insertInfo("B", 1, "192.168.1.1");
        fs.insertInfo("B", 2, "192.168.1.2");
        fs.insertInfo("B", 3, "192.168.1.4");
        fs.insertInfo("B", 1, "192.168.1.6");
        fs.insertInfo("B", 2, "192.168.1.1");
        fs.insertInfo("B", 3, "192.168.1.2");

        // File C
        fs.insertInfo("C", 1, "192.168.1.1");
        fs.insertInfo("C", 2, "192.168.1.2");
        fs.insertInfo("C", 3, "192.168.1.3");
        fs.insertInfo("C", 4, "192.168.1.1");
        fs.insertInfo("C", 5, "192.168.1.2");
        fs.insertInfo("C", 6, "192.168.1.3");
        fs.insertInfo("C", 7, "192.168.1.4");
        fs.insertInfo("C", 1, "192.168.1.3");
        fs.insertInfo("C", 2, "192.168.1.4");
        fs.insertInfo("C", 3, "192.168.1.7");
        fs.insertInfo("C", 4, "192.168.1.2");
        fs.insertInfo("C", 5, "192.168.1.4");
        fs.insertInfo("C", 6, "192.168.1.5");
        fs.insertInfo("C", 7, "192.168.1.1");

        // File D
        fs.insertInfo("D", 1, "192.168.1.6");
        fs.insertInfo("D", 2, "192.168.1.6");
        fs.insertInfo("D", 1, "192.168.1.1");
        fs.insertInfo("D", 2, "192.168.1.2");
        fs.insertInfo("D", 1, "192.168.1.3");
        fs.insertInfo("D", 2, "192.168.1.4");

        // File E
        fs.insertInfo("E", 1, "192.168.1.7");
        fs.insertInfo("E", 1, "192.168.1.1");
        fs.insertInfo("E", 1, "192.168.1.3");

        // File F
        fs.insertInfo("F", 1, "192.168.1.2");
        fs.insertInfo("F", 2, "192.168.1.1");
        fs.insertInfo("F", 3, "192.168.1.2");
        fs.insertInfo("F", 4, "192.168.1.6");
        fs.insertInfo("F", 5, "192.168.1.3");
        fs.insertInfo("F", 1, "192.168.1.4");
        fs.insertInfo("F", 2, "192.168.1.6");
        fs.insertInfo("F", 3, "192.168.1.1");
        fs.insertInfo("F", 4, "192.168.1.3");
        fs.insertInfo("F", 5, "192.168.1.4");



        Timer timer = new Timer();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                fs.verifyTimeStamp();
            }
        };

        timer.scheduleAtFixedRate(task, 0, 3000);

        System.out.println(fs.memoryToString());
        System.out.println(fs.timeToString());

        System.out.println("//////////////");
        fs.pickFile("F");
        System.out.println("//////////////");
        System.out.println(fs.memoryToString());


        //List<String> myList = new ArrayList<>();
        //myList.add("1:2,3,4");
        // myList.add("5:3,4,5");
        //myList.add("2:2");



        /*Node nd = new Node("AA",myList);
        nd.convertToString();

        fs.messageParser("MEUIP|2|file1:223,3,44;file3:5,4,2;");

        */


    }
}
