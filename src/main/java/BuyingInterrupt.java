public class BuyingInterrupt extends Object implements Runnable {
    private volatile boolean isAlive = true;

    public void stopThread(){
        this.isAlive = false;
    }

    public void buying(int cnt){
        System.out.println("buying ticket-"+(cnt));
//        int tickets = 20;
//        for(int i=0;i<20;i++){
//                if(tickets>0)
//                    System.out.println("buying ticket-"+(tickets--));
//        }

    }

    public void run(){
        int cnt =0;
        while(!Thread.currentThread().isInterrupted()){
            buying(cnt++);
        }
//        while(this.isAlive){
//            buying();
//        }
    }
    public static void main(String[] args) throws InterruptedException {
        BuyingInterrupt bi = new BuyingInterrupt();
        Thread thread = new Thread(bi);
        System.out.println("start main thread");
        thread.start();
        Thread.sleep(1);
        System.out.println("\nmain thread interrupt\n");
        thread.interrupt();
        thread.join(); // 等待t线程结束
        System.out.println("ending");
    }
}
