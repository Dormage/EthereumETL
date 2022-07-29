public class Config {
    public String databaseUser;
    public String databasePassword;
    public String databaseAddress;
    public Integer databasePort;
    public String databaseName;
    public int startBlock;
    public int endBlock;
    public int collectionId;
    public int targetLevel;
    public String providerUrl; //https://mainnet.infura.io/v3/32a08700bc2c4012aead1ac416d4dac0
    public int maxWorkers;
    public int batchSize;
    public boolean ipc;
    public String ipcProvider;
    public boolean useTransactionBuffer;
    public boolean readFile;
    public String transactionsFile;
    public boolean splitFile;
    public int producers;
    public long splitSize;
    public int batchLimit;
    public String outputFile;

    @Override
    public String toString() {
        return "Config{" +
                "databaseUser='" + databaseUser + '\'' +
                ", databasePassword='" + databasePassword + '\'' +
                ", databaseAddress='" + databaseAddress + '\'' +
                ", databasePort=" + databasePort +
                ", databaseName='" + databaseName + '\'' +
                ", startBlock=" + startBlock +
                ", endBlock=" + endBlock +
                '}';
    }
}
