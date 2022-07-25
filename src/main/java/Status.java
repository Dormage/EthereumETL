import java.util.TimerTask;

public class Status extends TimerTask {
    long totalTrasactions;
    long queueSize;
    long currentTransactions;
    int level;
    private int ticks;
    private int newVertices;
    private int totalVertices;


    public Status() {
        totalTrasactions = 0;
        currentTransactions = 0;
        queueSize = 0;
        level = 0;
        newVertices = 0;
        totalVertices = 0;
        ticks = 1;
    }

    @Override
    public void run() {
        System.out.print(Constants.STATUS + "Processed: " + currentTransactions
                + " Total: " + totalTrasactions
                + " Avg " + (totalTrasactions / ticks) + " /s"
                + " Vertices new: " + newVertices
                + " Vertices total: " + totalVertices
                + " Level: " + level
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
}
