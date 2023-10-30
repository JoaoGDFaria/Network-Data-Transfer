import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;


public class Main {
    public static void main(String[] args){

        FS_Tracker fs = new FS_Tracker();
        fs.insertInfo("A",1,"A");
        fs.insertInfo("A",1,"B");
        fs.insertInfo("S",1,"A");
        fs.insertInfo("S",1,"B");
        fs.insertInfo("D",1,"E");



        Timer timer = new Timer();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                fs.verifyTimeStamp();
            }
        };

        timer.scheduleAtFixedRate(task, 0, 3000);

        System.out.println(fs.toString());



        List<String> myList = new ArrayList<>();
        myList.add("1:2,3,4");
        myList.add("5:3,4,5");
        myList.add("2:2");

        Node nd = new Node("AA",myList);
        nd.convertToString();

        fs.messageParser("MEUIP|2|file1:223,3,44;file3:5,4,2;");


    }
}
