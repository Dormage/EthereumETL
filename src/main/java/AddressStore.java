import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

public class AddressStore {

    // each cell in the array list is a store for the addresses of the layer
    public ArrayList<HashSet<String>> store = new ArrayList<>();

    private Config config;
    public Connection conn;

    private int currentLevel;



    public AddressStore(Config config){
        this.config = config;
        this.connectDatabase();
        this.store.add(new HashSet<String>()); //create layer 0
        this.fetchAddresses();
        this.store.add(new HashSet<String>()); // create layer 1
        this.currentLevel = 1;
    }


    public int getCurrentLevel(){
        return this.currentLevel;
    }

    /*

    Can be ran concurrently since we will fill this layer by layer
    we are filling now layer n, to fill layer n we need to fetch all transactions
    from addresses in layer n-1,
    here we look if the address is in layer n-1 if it is, fetch the transaction(this not here)
    we are not interested if the address is in layer < n-1 since if it is we have already fetched its transactions
    we can look at this concurrently, since when we lookup n-1 we are inserting addresses in layer n using the function add()
     */

    public boolean contains(String address){
        return this.store.get(this.store.size()-2).contains(address);
    }


    public boolean contains(String address, int level){
        return this.store.get(level).contains(address);
    }

    /*
    This need to be synchronized
    verify if the address is already in one of the other layers of the store
    If not then insert the address in layer n
     */

    public synchronized void add(String address){
        for (HashSet<String> level:this.store) { // verify if in one of the layers
            if(level.contains(address)){
                return;
            }
        }
        this.store.get(this.store.size()-1).add(address); // if not insert
    }

    /*
    Simply create a new lavel in the store
     */

    public void createLevel(){
        this.store.add(new HashSet<String>()); // add layer n+1
        currentLevel++;
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


    public void fetchAddresses(){
        String sql = "SELECT `address` FROM `Address` WHERE `isSmartContract` = 0 and `cid` = ?";
        try {
            PreparedStatement pstmt = this.conn.prepareStatement(sql);
            pstmt.setInt(1,config.collectionId);
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                // ... do something with result set
                //System.out.println(resultSet.getString(1));
                store.get(0).add(resultSet.getString(1));
            }
            System.out.println("Level 0 is including: "+store.get(0).size() + " addresses");
        } catch (SQLException e) {
            System.out.println("Failed to fetch addresses: " + e.getMessage());
            e.printStackTrace();
        }
    }




}
