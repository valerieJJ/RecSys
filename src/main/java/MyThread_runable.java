public class MyThread_runable implements Runnable {
    private int ticket = 5;
    private String name = "";
    public MyThread_runable(String name){
        this.name = name;
    }
    public void run(){
        for(int i=0;i<10;i++) {
            if (ticket > 0)
                System.out.println(this.name+"'s running to buy ticket-" + (ticket--));
        }
    }
}
