
public class Producer implements Runnable {
    private final DataQueue dataQueue;
    private volatile boolean runFlag;
    private Config config;

    private static int idSequence = 0;

    public Producer(DataQueue dataQueue, Config config) {
        this.dataQueue = dataQueue;
        this.config = config;
        runFlag = true;
    }

    @Override
    public void run() {
        produce();
    }

    public void produce() {
        while (runFlag) {
            Block block = readBlock();
            while (dataQueue.isFull()) {
                try {
                    dataQueue.waitOnFull();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            if (!runFlag) {
                break;
            }
            dataQueue.add(block);
            dataQueue.notifyAllForEmpty();
        }
        System.out.println("Producer Stopped");
    }

    private Block readBlock() {
        Block block = new Block();
        return block;
    }

    public void stop() {
        runFlag = false;
        dataQueue.notifyAllForFull();
    }
}