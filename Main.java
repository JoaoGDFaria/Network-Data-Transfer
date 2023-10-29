import java.time.LocalTime;
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
    }
}
