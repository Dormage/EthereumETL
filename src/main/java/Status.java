import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

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

    public Object monitor;

    private int working;

    public int dbInsertions;

    BlockingQueue<ArrayList<ByteStructure>> queue;

    public Status(Config config, BlockingQueue<ArrayList<ByteStructure>> queue) {
        totalTrasactions = 0;
        currentTransactions = 0;
        queueSize = 0;
        level = 0;
        newVertices = 0;
        totalVertices = 0;
        ticks = 1;
        currentBlock = 1;
        dbInsertions = 0;
        this.config = config;
        this.queue = queue;
        monitor = new Object();
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
                //+ " CurrentBLock: " + currentBlock
               // + " Progress: " + Math.round(progress) + " %"
                + " QueueSize: " + queueSize
                + " DB insertions: " + dbInsertions
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

    public synchronized void newInsertion() {dbInsertions++;
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

    public synchronized void startWork(){
        this.working++;
        queueSize = queue.size();
    }
    public synchronized void endWork(){
        synchronized (monitor) {
            this.working--;
            if (working < 1 && queue.isEmpty()) {
                monitor.notifyAll();
            }
        }
    }

    public boolean areWorking(){
        if(this.working>0){
            return true;
        }
        return false;
    }

    public int getCurrentBlock(){
        return this.currentBlock;
    }
}
