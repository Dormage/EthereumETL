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

    public Consumer(BlockingQueue<Transaction> queue, Config config, Status status) {
        this.queue = queue;
        runFlag = true;
        this.config = config;
        this.status = status;
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
        if (transaction != null) {
            //parse and insert into DB
        }
    }

    public void stop() {
        runFlag = false;
    }
}