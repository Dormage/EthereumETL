import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class Consumer implements Runnable {
    private final BlockingQueue<Transaction> queue;
    private volatile boolean runFlag;
    private Config config;
    private Connection conn;
    private Status status;
    private AddressStore addressStore;

    public Consumer(BlockingQueue<Transaction> queue, Config config, Status status, AddressStore addressStore) {
        this.queue = queue;
        runFlag = true;
        this.config = config;
        this.status = status;
        this.addressStore = addressStore;
    }

    @Override
    public void run() {
        consume();
    }

    public void connectDatabase() {
        Properties connectionProps = new Properties();
        connectionProps.put("user", config.databaseUser);
        connectionProps.put("password", config.databasePassword);
        try {
            conn = DriverManager.getConnection(
                    "jdbc:mysql://" +
                            config.databaseAddress +
                            ":" + config.databasePort + "/" + config.databaseName,
                    connectionProps);
        } catch (SQLException e) {
            System.out.println(Constants.NETWORK + "Failed to connected to database: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println(Constants.NETWORK + Thread.currentThread().getName() + " Connected to database");
    }

    public void consume() {
        connectDatabase();
        while (runFlag) {
            try {
                Transaction transaction = queue.take();
                parseTransaction(transaction);
                status.newTransaction();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(Constants.INFO + "Consumer Stopped");
    }

    private void parseTransaction(Transaction transaction) {
        if (transaction != null && !transaction.input.equals("0x") && transaction.value.compareTo(BigInteger.ZERO) > 0) {
            boolean from = addressStore.contains(transaction.from_address, status.level);
            boolean to = addressStore.contains(transaction.to_address, status.level);
            if (to && !from) {
                addressStore.add(transaction.to_address);
                status.newVertex();
            }
            if (from && !to) {
                addressStore.add(transaction.from_address);
                status.newVertex();
            }
            if (!from && !to) {
            }
        }
    }

    private void insertTransaction(Transaction transaction) {

    }

    public void stop() {
        runFlag = false;
    }
}