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

    public boolean readFile;

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
