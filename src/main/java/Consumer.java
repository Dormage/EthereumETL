import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class Consumer implements Runnable {
    private final BlockingQueue<ArrayList<ByteStructure>> queue;
    private volatile boolean runFlag;
    private Config config;
    private Connection conn;
    private Status status;
    private AddressStore addressStore;

    int count = 0;

    private ArrayList<ByteStructure> bufferOfBuffers;

    public Consumer(BlockingQueue<ArrayList<ByteStructure>> queue, Config config, Status status, AddressStore addressStore) {
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
            //conn.setAutoCommit(false); // set autocommit off for concurrent READ & WRITE
            //conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
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
                bufferOfBuffers = queue.take();
                status.startWork();
                parseBuffer(bufferOfBuffers);
                status.endWork();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(Constants.INFO + "Consumer Stopped");
    }


    private void parseBuffer(ArrayList<ByteStructure> bufferOfBuffers){
        status.queueSize = queue.size();
        //System.out.println(Thread.currentThread().getName() +" BufferId: "+ bufferOfBuffers.get(0).bufferId);
        for (ByteStructure buffer:bufferOfBuffers ) {
            StringBuilder stringTransaction = new StringBuilder();
            //System.out.println("offests: "+buffer.startOffset+" e: "+buffer.endOffset);
            for (long i = buffer.startOffset; i < buffer.endOffset; i++) {
                char c = (char) buffer.buffer[(int)i];

                if ('\n' == c) {
                    status.newTransaction();
                    parseTransaction(new Transaction(stringTransaction.toString().split(",")));
                    stringTransaction = new StringBuilder();
                }else{
                    stringTransaction.append(c);
                }
            }
        }

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
                        insertIntoDB(transaction);
                        status.newVertex();
                        break;
                    case "false--true":
                        addressStore.add(transaction.from_address);
                        insertIntoDB(transaction);
                        status.newVertex();

                        break;
                    case "true--true":
                        insertIntoDB(transaction);
                        break;
                }
            }
        }
    }

    private void insertIntoDB(Transaction transaction) {
        try
        {
            insertTransaction(transaction);
/*            conn.setAutoCommit(false); // set autocommit off for concurrent READ & WRITE
            //System.out.println(Thread.currentThread().getName() +" insert: "+ transaction.toString());
            if(!transactionExists(transaction)){
                status.newInsertion();
                insertTransaction(transaction);
            }
            conn.commit();*/
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


    public boolean transactionExists(Transaction transaction) throws SQLException {
        String sql = "SELECT EXISTS(SELECT * FROM `Transactions` WHERE `cid` = ? and `transaction_hash` = ? and `log_index` IS NULL )";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1,config.collectionId);
        pstmt.setString (2, transaction.hash);

        ResultSet resultSet = pstmt.executeQuery();

        if (resultSet.next()) {
            boolean exists = resultSet.getBoolean(1);
            if (exists) {
                return true;
            }
        }
        //System.out.println(transaction.toString());
        return false;
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
        preparedStmt.execute();
    }


    private double weiToEth(BigInteger amount){
        // one eth is 10**18 wei
        // remove last 12 digits and convert to long
        if(amount.toString().length()>12){
            long value = Long.parseLong(amount.toString().substring(0,amount.toString().length()-12));
            return value / (Math.pow(10,6));
        }else{
            return 0;
        }
    }

    private long weiToGwei(BigInteger amount){
        long gwei = Long.parseLong(amount.divide(new BigInteger("1000000000")).toString());
        return gwei;
    }



    public void stop() {
        runFlag = false;
    }
}