import com.google.gson.Gson;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
    private ProcessBuilder builder;
    private Process process;
    private InputStream in;
    private List<Transaction> buffer;

    public Producer(BlockingQueue<Transaction> queue, Config config, Status status, AddressStore addressStore) {
        this.queue = queue;
        this.config = config;
        runFlag = true;
        this.gson = new Gson();
        this.currentBlock = config.startBlock;
        this.status = status;
        this.addressStore = addressStore;
        this.currentBlock = config.startBlock;
    }

    @Override
    public void run() {
        if(config.readFile) {
            startReadingCSV();
        }else {
            startEtlProcess();
        }
        try {
            produce();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startEtlProcess() {
        String[] command = null;
        try {
            if (config.ipc) {
                command = new String[]{"ethereumetl", "stream",
                        "-p", config.ipcProvider,
                        "--start-block", "" + config.startBlock,
                        "--batch-size", "" + config.batchSize,
                        "--max-workers", "" + config.maxWorkers,
                        "-e", "transaction"};
            } else {
                command = new String[]{"ethereumetl", "stream",
                        "--provider-uri", config.providerUrl,
                        "--start-block", "" + config.startBlock,
                        "--batch-size", "" + config.batchSize,
                        "--max-workers", "" + config.maxWorkers,
                        "-e", "transaction"};
            }
            builder = new ProcessBuilder(command);
            builder.redirectError(ProcessBuilder.Redirect.DISCARD);
            process = builder.start();
            in = process.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(in));
            buffer = new ArrayList<>(1001);
        } catch (IOException e) {
            System.out.println(Constants.ERROR + "Failed to open input stream to external process!");
            e.printStackTrace();
        }
    }


    public void startReadingCSV(){
        try {
        bufferedReader = Files.newBufferedReader(Paths.get(config.transactionsFile));
        bufferedReader.readLine(); // throw away the first line (csv header)
        buffer = new ArrayList<>(1001);
        } catch (IOException e) {
            System.out.println(Constants.ERROR + "Failed to open input stream to csv file!");
            e.printStackTrace();
        }
    }

    public void produce() throws IOException {
        String line;
        while (runFlag) {
            while ((line = bufferedReader.readLine()) != null) {
                Transaction transaction = null;
                if(config.readFile){
                    transaction = new Transaction(line.split(","));
                }else {
                    transaction = gson.fromJson(line, Transaction.class);
                }
                if (currentBlock < transaction.block_number) {
                    currentBlock++;
                    status.newBlock();
                }
                if (config.useTransactionBuffer) {
                    //
                    if (buffer.size() < 1000) {
                        buffer.add(transaction);
                    } else {
                        queue.addAll(buffer);
                        buffer.clear();
                    }
                }else{
                    queue.offer(transaction);
                }
                status.queueSize = queue.size();
                if (currentBlock < transaction.block_number) {
                    currentBlock++;
                    status.newBlock();
                }
                if (transaction.block_number > config.endBlock) {
                    while (!queue.isEmpty()) {}
                    if (status.level < config.targetLevel) {
                        addressStore.createLevel();
                        status.level++;
                        status.resetCurrentBlock();
                        if(!config.readFile) {
                            stopEtlProcess();
                            startEtlProcess();
                        }else {
                            bufferedReader.close();
                            startReadingCSV();
                        }
                        System.out.println(Constants.SUCCESS + "Total number of wallets added: " + addressStore.store.get(status.level - 1).size());
                    } else {
                        System.out.println(Constants.SUCCESS + "Completed!");
                        stop();
                        if(!config.readFile) {
                            stopEtlProcess();
                        }else {
                            bufferedReader.close();
                        }
                        break;
                    }
                }
            }
        }
        System.out.println(Constants.INFO + "Producer Stopped");
    }

    private void stopEtlProcess() {
        try {
            in.close();
            process.destroy();
            System.out.println(Constants.INFO + "ETL process shut down!");
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Files.delete(Paths.get("last_synced_block.txt"));
            System.out.println(Constants.SUCCESS + "Deleted previous files: ");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        runFlag = false;
    }
}