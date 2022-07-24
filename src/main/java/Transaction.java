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
