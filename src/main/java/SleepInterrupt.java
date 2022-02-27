import java.lang.*;
import java.lang.InterruptedException;
import java.util.concurrent.TimeUnit;

public class SleepInterrupt extends Object implements Runnable {
    void buy(){
        int tickets = 100;
        for(int i=0;i<10;i++){
            if (tickets > 0)
                System.out.println("buying " +(tickets--));
        }
    }

    public void sleep() {
        long t0 = System.nanoTime();
//        buy();
        try{
            System.out.println("in sleep() - about to sleep for 20 seconds");
            Thread.sleep(20000);
            long t1 = System.nanoTime();
            System.out.println(String.format("in sleep() - woke up, time-{}", (t1-t0)));
        }catch (InterruptedException e) {
            long t1 = System.nanoTime();
            System.out.println("in sleep() - interrupted while sleeping, sleep time:"+ TimeUnit.SECONDS.convert((t1-t0), TimeUnit.NANOSECONDS));
            return;
        }
    }

    public void run() {
//        sleep();

    }


    public static void test_sleep_interrupt() throws InterruptedException {
        SleepInterrupt si = new SleepInterrupt();
        Thread t = new Thread(si);
        t.start();
        try{
            //主线程休眠2秒，从而确保刚才启动的线程有机会执行一段时间
            Thread.sleep(5000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println("in main() - interrupt other thread");
        t.interrupt();
        t.join(); // 等待t线程结束
        System.out.println("in main() - leaving");
    }

    public static void main(String[] args) throws InterruptedException {
        test_sleep_interrupt();

    }
}
