package BlockingQueue;

import java.util.concurrent.BlockingQueue;


public class producer implements Runnable {
    private BlockingQueue<Integer> blockingQueue;
    private static Integer index = 0;
    private int num = 0;

    public producer(BlockingQueue<Integer> blockingQueue, int num) {
        this.blockingQueue = blockingQueue;
        this.num = num;
    }

    @Override
    public void run() {
        while (true) {
            try {
                index++;
                blockingQueue.put(index);
                System.out.println(num + "  produce： " + index);
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}