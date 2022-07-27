import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

public class Status extends TimerTask {
    long totalTransactions;
    volatile long trxQueueSize;
    long currentTransactions;
    int level;
    private int ticks;
    private int newVertices;
    private int totalVertices;
    private int currentBlock;
    private Config config;

    public volatile int lineQueueSize;
    public BlockingQueue<String> lineQueue;
    public BlockingQueue<Transaction> insertionQueue;

    public int inserted;

    public Status(Config config, BlockingQueue<String> lineQueue,BlockingQueue<Transaction> insertionQueue) {
        totalTransactions = 0;
        currentTransactions = 0;
        trxQueueSize = 0;
        lineQueueSize = 0;
        level = 0;
        newVertices = 0;
        totalVertices = 0;
        ticks = 1;
        currentBlock = 1;
        this.config = config;
        inserted = 0;
        this.lineQueue=lineQueue;
        this.insertionQueue = insertionQueue;

    }

    @Override
    public void run() {
        float progress = ((currentBlock *1f / (config.endBlock - config.startBlock) * 100));
        System.out.print(Constants.STATUS + "Processed: " + currentTransactions
                + " Total: " + totalTransactions
                + " Avg " + (totalTransactions / ticks) + " /s"
                + " Vertices new: " + newVertices
                + " Vertices total: " + totalVertices
                + " Level: " + level
                + " CurrentBLock: " + currentBlock
                + " Progress: " + Math.round(progress) + " %"
                + " Line queueSize: " + lineQueue.size()
                + " Transaction queueSize: " + insertionQueue.size()
                + " Inserted in DB: " + inserted
                + " \r");
        reset();
    }

    private void reset() {
        totalTransactions += currentTransactions;
        currentTransactions = 0;
        totalVertices += newVertices;
        newVertices = 0;
        ticks++;
    }

    public void newTransaction() {
        currentTransactions++;
    }

    public void newVertex() {
        newVertices++;
    }

    public void newBlock() {
        currentBlock++;
    }

    public void resetCurrentBlock() {
        currentBlock = 1;
    }

    public int getCurrentBlock(){
        return this.currentBlock;
    }

}
