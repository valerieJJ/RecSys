
public class MyThread extends Thread{
    private int ticket = 5;
    private String name = "";
    public MyThread(String name){
        this.name = name;
    }
    public void run(){
        for(int i=0;i<10;i++) {
            if (ticket > 0)
                System.out.println(this.name+"'s buying ticket-" + (ticket--));
        }
    }
}
