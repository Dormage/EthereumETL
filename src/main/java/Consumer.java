
public class Consumer implements Runnable {
    private final DataQueue dataQueue;
    private volatile boolean runFlag;

    public Consumer(DataQueue dataQueue) {
        this.dataQueue = dataQueue;
        runFlag = true;
    }

    @Override
    public void run() {
        consume();
    }

    public void consume() {
        while (runFlag) {
            Block block;
            if (dataQueue.isEmpty()) {
                try {
                    dataQueue.waitOnEmpty();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            if (!runFlag) {
                break;
            }
            block = dataQueue.remove();
            dataQueue.notifyAllForFull();
            parseBlock(block);
        }
        System.out.println("Consumer Stopped");
    }

    private void parseBlock(Block block) {
        if (block != null) {
            //parse and insert into DB
        }
    }

    public void stop() {
        runFlag = false;
        dataQueue.notifyAllForEmpty();
    }
}