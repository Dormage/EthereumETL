import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class Consumer implements Runnable {
    private final BlockingQueue<Transaction> queue;
    private volatile boolean runFlag;
    private Config config;
    public Connection conn;

    public Consumer(BlockingQueue<Transaction> queue, Config config) {
        this.queue = queue;
        runFlag = true;
        this.config = config;
    }

    @Override
    public void run() {
        consume();
    }

    public void connectDatabase(){
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
        System.out.println(Constants.NETWORK + Thread.currentThread().getName() +" Connected to database");
    }

    public void consume() {
        connectDatabase();
        while (runFlag) {
            try {
            Transaction transaction = queue.take();
            System.out.println(Constants.SUCCESS+"Got new transaction " +transaction);
            parseTransaction(transaction);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(Constants.INFO+"Consumer Stopped");
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