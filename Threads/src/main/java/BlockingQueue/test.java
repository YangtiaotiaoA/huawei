package BlockingQueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class test {
    public static void main(String[] args) {
        BlockingQueue blockingQueue = new ArrayBlockingQueue<>(10);
        new Thread(new producer(blockingQueue, 1)).start();
        new Thread(new producer(blockingQueue,2)).start();

        new Thread(new consumer(blockingQueue,1)).start();
        new Thread(new consumer(blockingQueue,2)).start();

    }
}