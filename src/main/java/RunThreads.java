
// https://wiki.jikexueyuan.com/project/java-concurrency/thread-interrupt.html

public class RunThreads {
    public static void runMyThreads(){
        new MyThread("mike").start();
        new MyThread("jack").start();
        new MyThread("diana").start();
    }
    public static void runableThreads(){
        // 三个线程共同买了5张票
        MyThread_runable thread1 = new MyThread_runable("billie");
        new Thread(thread1).start();
        new Thread(thread1).start();
        new Thread(thread1).start();
    }

    public static void main(String[] args){
        runableThreads();
    }
}
