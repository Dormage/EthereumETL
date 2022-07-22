import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Consumer implements Runnable {
    private final DataQueue dataQueue;
    private volatile boolean runFlag;
    private Config config;
    public Connection conn;

    public Consumer(DataQueue dataQueue, Config config) {
        this.dataQueue = dataQueue;
        runFlag = true;
        this.config = config;
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
            System.out.println(Constants.NETWORK     + "Failed to connected to database: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println(Constants.NETWORK + "Connected to database");
    }

    @Override
    public void run() {
        consume();
    }

    public void consume() {
        while (runFlag) {
            Transaction transaction;
            if (dataQueue.isEmpty()) {
                try {
                    dataQueue.waitOnEmpty();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            if (!runFlag) {
                break;
            }
            transaction = dataQueue.remove();
            System.out.println(Constants.SUCCESS+"Got new transaction " +transaction.hash);
            dataQueue.notifyAllForFull();
            parseTransaction(transaction);
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
        dataQueue.notifyAllForEmpty();
    }
}