import java.math.BigInteger;

public class Transaction {
    String type;
    String hash;
    int nonce;
    int transaction_index;
    String from_address;
    String to_address;
    BigInteger value;
    BigInteger gas;
    BigInteger gas_price;
    String input;
    long block_timestamp;
    long block_number;
    String block_hash;
    BigInteger max_fee_per_gas;
    BigInteger max_priority_fee_per_gas;
    int transaction_type;
    BigInteger receipt_cumulative_gas_used;
    BigInteger receipt_gas_used;
    String receipt_contract_address;
    String receipt_root;
    int receipt_status;
    BigInteger receipt_effective_gas_price;
    String item_id;
    String item_timestamp;

    int level;

    public Transaction(String[] line){
            this.hash = line[0];
            this.nonce = Integer.parseInt(line[1]);
            this.block_hash = line[2];
            this.block_number = Long.parseLong(line[3]);
            this.transaction_index = Integer.parseInt(line[4]);
            this.from_address = line[5];
            this.to_address = line[6];
            this.value = new BigInteger(line[7]);
            this.gas = new BigInteger(line[8]);
            this.gas_price = new BigInteger(line[9]);
            this.input = line[10];
            this.block_timestamp = Long.parseLong(line[11]) * 1000;
            //this.max_fee_per_gas = new BigInteger((line[12].equals("")) ? "0" : line[12]);
            //this.max_priority_fee_per_gas = new BigInteger((line[13].equals("")) ? "0" : line[13]);
            this.transaction_type = 0;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "hash='" + hash + '\'' +
                ", from_address='" + from_address + '\'' +
                ", to_address='" + to_address + '\'' +
                ", block_number=" + block_number +
                ", block_hash='" + block_hash + '\'' +
                ", value='" + value + '\'' +
                '}';
    }



}
