import java.math.BigInteger;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class Consumer implements Runnable {
    private final BlockingQueue<String> lineQueue;

    private final BlockingQueue<Transaction> insertionQueue;
    private volatile boolean runFlag;
    private Config config;
    private Status status;
    private AddressStore addressStore;

    public Consumer(BlockingQueue<String> lineQueue,BlockingQueue<Transaction> insertionQueue, Config config, Status status, AddressStore addressStore) {
        this.lineQueue = lineQueue;
        this.insertionQueue = insertionQueue;
        runFlag = true;
        this.config = config;
        this.status = status;
        this.addressStore = addressStore;
    }

    @Override
    public void run() {
        consume();
    }



    public void consume() {
        while (runFlag) {
            try {
                Transaction transaction = new Transaction(lineQueue.take().split(","),addressStore.getCurrentLevel()-1);
                parseTransaction(transaction);
                status.newTransaction();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(Constants.INFO + "Consumer Stopped");
    }

    private void parseTransaction(Transaction transaction) {
        if (transaction != null) {
            //parse and insert into DB
            //filter out non interesting transactions
            if(transaction.value.compareTo(new BigInteger("0")) != 0 && transaction.input.compareTo("0x") == 0){
                String lookup = String.valueOf(addressStore.contains(transaction.from_address)) + "--" + String.valueOf(addressStore.contains(transaction.to_address));
                switch (lookup){
                    case "true--false":
                        addressStore.add(transaction.to_address);
                        insertionQueue.offer(transaction);
                        status.newVertex();
                        break;
                    case "false--true":
                        addressStore.add(transaction.from_address);
                        insertionQueue.offer(transaction);
                        status.newVertex();
                        break;
                    case "true--true":
                        insertionQueue.offer(transaction);
                        break;
                }
                status.lineQueueSize = lineQueue.size();
            }
        }
    }




    public void stop() {
        runFlag = false;
    }
}