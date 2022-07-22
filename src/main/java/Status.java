import java.util.TimerTask;

public class Status extends TimerTask {
    long totalTrasactions;
    long queueSize;
    long currentTransactions;
    int debth;
    private int ticks;


    public Status() {
        totalTrasactions = 0;
        currentTransactions = 0;
        queueSize = 0;
        debth = 0;
        ticks = 1;
    }

    @Override
    public void run() {
        System.out.print(Constants.STATUS + "Processed: " + currentTransactions + " Total: " + totalTrasactions + " Avg " + (totalTrasactions /ticks) + " /s \r");;
        reset();
    }

    private void reset() {
        totalTrasactions += currentTransactions;
        currentTransactions = 0;
        ticks++;
    }

    public synchronized void newTransaction() {
        currentTransactions++;
    }
}
