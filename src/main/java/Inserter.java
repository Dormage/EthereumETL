import java.math.BigInteger;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class Inserter implements  Runnable{

    private final BlockingQueue<Transaction> insertionQueue;
    private volatile boolean runFlag;
    private Config config;
    private Status status;
    private AddressStore addressStore;
    public Connection conn;

    private HashSet<String> insertedTransactions = new HashSet<String>();

    private PreparedStatement batchStatement;

    private int batchSize = 0;

    public Inserter(BlockingQueue<Transaction> insertionQueue, Config config, Status status, AddressStore addressStore) {
        this.insertionQueue = insertionQueue;
        runFlag = true;
        this.config = config;
        this.status = status;
        this.addressStore = addressStore;
    }

    public void run(){
        connectDatabase();
        feed();
    }

    public void feed(){
        while (runFlag) {
            try {
                Transaction transaction = insertionQueue.take();
                insertIntoDB(transaction);
                status.trxQueueSize = insertionQueue.size();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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
            //conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            //conn.setAutoCommit(false); // set autocommit off for concurrent READ & WRITE
        } catch (SQLException e) {
            System.out.println(Constants.NETWORK + "Failed to connected to database: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println(Constants.NETWORK + Thread.currentThread().getName() + " Connected to database");
    }


    private void insertIntoDB(Transaction transaction) {
        try
        {
            if(!alreadyInserted(transaction)){
                insertBatch(transaction);
                status.inserted++;
            }
        }
        catch (Exception e)
        {
            System.out.println("Failed to insert transaction in DB" + e.getMessage());
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.out.println("Failed rollback" + ex.getMessage());
                ex.printStackTrace();
            }
        }
    }

    public void insertBatch(Transaction transaction) throws SQLException{

        if(batchSize<1){
            // the mysql insert statement
            String query = "INSERT INTO Transactions (`cid`, `amount`, `currency`, `from`, `to`, `gas_price`, `gas_used`, `timestamp`, `transaction_hash`, `level`, `block_no`, `log_index`)"
                    + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            // create the mysql insert preparedstatement
            batchStatement = conn.prepareStatement(query);
        }

        batchStatement.setInt    (1, config.collectionId);
        batchStatement.setDouble   (2, weiToEth(transaction.value));
        batchStatement.setString (3, "ETH");
        batchStatement.setString (4, transaction.from_address);
        batchStatement.setString (5, transaction.to_address);
        batchStatement.setLong    (6, Long.parseLong(transaction.gas_price.toString()));
        batchStatement.setLong    (7, Long.parseLong(transaction.gas.toString()));
        batchStatement.setTimestamp   (8, new Timestamp(transaction.block_timestamp));
        batchStatement.setString (9, transaction.hash);
        batchStatement.setInt    (10, transaction.level);
        batchStatement.setLong    (11, transaction.block_number);
        batchStatement.setNull   (12, Types.INTEGER); // important if parsing ERC-20

        batchStatement.addBatch();
        batchSize++;

        if(batchSize >= config.batchLimit || insertionQueue.isEmpty() ){
            int[] count = batchStatement.executeBatch();
            for (int c: count ) {
                if(c==0){
                    System.out.println("errors when inserting");
                }
            }
            batchSize = 0;
        }

    }

    public boolean alreadyInserted(Transaction transaction){
        if(insertedTransactions.contains(transaction.hash)){
            return true;
        }else {
            insertedTransactions.add(transaction.hash);
            return false;
        }
    }


    public boolean transactionExists(Transaction transaction) throws SQLException {
        String sql = "SELECT * FROM `Transactions` WHERE `cid` = ? and `transaction_hash` = ? and `log_index` IS NULL ";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1,config.collectionId);
        pstmt.setString (2, transaction.hash);

        ResultSet resultSet = pstmt.executeQuery();

        if (!resultSet.next() ) {
            return false;
        }
        return true;
    }

    private void insertTransaction(Transaction transaction) throws SQLException {
        // the mysql insert statement
        String query = "INSERT INTO Transactions (`cid`, `amount`, `currency`, `from`, `to`, `gas_price`, `gas_used`, `timestamp`, `transaction_hash`, `level`, `block_no`, `log_index`)"
                + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // create the mysql insert preparedstatement
        PreparedStatement preparedStmt = conn.prepareStatement(query);

        preparedStmt.setInt    (1, config.collectionId);
        preparedStmt.setDouble   (2, weiToEth(transaction.value));
        preparedStmt.setString (3, "ETH");
        preparedStmt.setString (4, transaction.from_address);
        preparedStmt.setString (5, transaction.to_address);
        preparedStmt.setLong    (6, Long.parseLong(transaction.gas_price.toString()));
        preparedStmt.setLong    (7, Long.parseLong(transaction.gas.toString()));
        preparedStmt.setTimestamp   (8, new Timestamp(transaction.block_timestamp));
        preparedStmt.setString (9, transaction.hash);
        preparedStmt.setInt    (10, addressStore.getCurrentLevel()-1);
        preparedStmt.setLong    (11, transaction.block_number);
        preparedStmt.setNull   (12, Types.INTEGER); // important if parsing ERC-20

        // execute the preparedstatement
        int count = preparedStmt.executeUpdate();
        if(count<1){
            System.out.println("errorres");
        }
    }



    private double weiToEth(BigInteger amount){
        // one eth is 10**18 wei
        // remove last 12 digits and convert to long
        if(amount.toString().length()<=12){
            return 0.0;
        }
        long value = Long.parseLong(amount.toString().substring(0,amount.toString().length()-12));
        return value / (Math.pow(10,6));
    }

    private long weiToGwei(BigInteger amount){
        long gwei = Long.parseLong(amount.divide(new BigInteger("1000000000")).toString());
        return gwei;
    }


}
