import java.util.TimerTask;

public class Status extends TimerTask {
    long totalTrasactions;
    long queueSize;
    long currentTransactions;
    int level;
    private int ticks;
    private int newVertices;
    private int totalVertices;
    private int currentBlock;
    private Config config;

    public Status(Config config) {
        totalTrasactions = 0;
        currentTransactions = 0;
        queueSize = 0;
        level = 0;
        newVertices = 0;
        totalVertices = 0;
        ticks = 1;
        currentBlock = 1;
        this.config = config;
    }

    @Override
    public void run() {
        float progress = ((currentBlock *1f / (config.endBlock - config.startBlock) * 100));
        System.out.print(Constants.STATUS + "Processed: " + currentTransactions
                + " Total: " + totalTrasactions
                + " Avg " + (totalTrasactions / ticks) + " /s"
                + " Vertices new: " + newVertices
                + " Vertices total: " + totalVertices
                + " Level: " + level
                + " CurrentBLock: " + currentBlock
                + " Progress: " + Math.round(progress) + " %"
                + " QueueSize: " + queueSize
                + " \r");
        reset();
    }

    private void reset() {
        totalTrasactions += currentTransactions;
        currentTransactions = 0;
        totalVertices += newVertices;
        newVertices = 0;
        ticks++;
    }

    public synchronized void newTransaction() {
        currentTransactions++;
    }

    public synchronized void newVertex() {
        newVertices++;
    }

    public synchronized void newBlock() {
        currentBlock++;
    }

    public void resetCurrentBlock() {
        currentBlock = 1;
    }
}
