package BlockingQueue;

import java.util.concurrent.BlockingQueue;


public class consumer implements Runnable {
    private BlockingQueue<Object> blockingQueue;
    private int num = 0;

    public consumer(BlockingQueue<Object> blockingQueue, int num) {
        this.blockingQueue = blockingQueue;
        this.num = num;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Object take = blockingQueue.take();
                System.out.println(num + "  consume：" + take);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
