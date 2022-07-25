import com.google.gson.Gson;

import java.io.*;
import java.util.concurrent.BlockingQueue;

public class Producer implements Runnable {
    private final BlockingQueue<Transaction> queue;
    private volatile boolean runFlag;
    private Config config;
    private BufferedReader bufferedReader;
    private static int idSequence = 0;
    private Gson gson;
    private int currentBlock;
    private Status status;
    private AddressStore addressStore;

    public Producer(BlockingQueue<Transaction> queue, Config config, Status status, AddressStore addressStore) {
        this.queue = queue;
        this.config = config;
        runFlag = true;
        this.gson = new Gson();
        this.currentBlock = config.startBlock;
        this.status = status;
        this.addressStore = addressStore;
    }

    @Override
    public void run() {
        startEtlProcess();
        try {
            produce();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startEtlProcess() {
        try {
            String[] command = {"ethereumetl", "stream",
                    "--provider-uri", "https://mainnet.infura.io/v3/32a08700bc2c4012aead1ac416d4dac0",
                    "--start-block", "" + config.startBlock,
                    "-e", "transaction"};
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.redirectError(ProcessBuilder.Redirect.DISCARD);
            Process process = builder.start();
            InputStream in = process.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(in));
        } catch (IOException e) {
            System.out.println(Constants.ERROR + "Failed to open input stream to external process!");
            e.printStackTrace();
        }
    }

    public void produce() throws IOException {
        String line;
        while (runFlag) {
            while ((line = bufferedReader.readLine()) != null) {
                Transaction transaction = gson.fromJson(line, Transaction.class);
                queue.offer(transaction);
                if (transaction.block_number > config.endBlock) {
                    if(status.level< config.targetLevel){
                        addressStore.createLevel();
                        status.level++;
                        startEtlProcess();
                    }else {
                        stop();
                    }
                }
            }
        }
        System.out.println(Constants.INFO + "Producer Stopped");
    }

    public void stop() {
        runFlag = false;
    }
}