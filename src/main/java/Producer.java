import com.google.gson.Gson;

import java.io.*;

public class Producer implements Runnable {
    private final DataQueue dataQueue;
    private volatile boolean runFlag;
    private Config config;
    private BufferedReader bufferedReader;
    private static int idSequence = 0;
    private Gson gson;
    private int currentBlock;

    public Producer(DataQueue dataQueue, Config config) {
        this.dataQueue = dataQueue;
        this.config = config;
        runFlag = true;
        this.gson = new Gson();
        this.currentBlock = config.startBlock;
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

    public void startEtlProcess(){
        try {
            String[] command = {"ethereumetl", "stream", "--provider-uri", "https://mainnet.infura.io/v3/32a08700bc2c4012aead1ac416d4dac0", "--start-block", ""+config.startBlock, "-e", "transaction"};
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.redirectErrorStream(true); // so we can ignore the error stream
            Process process = builder.start();
            InputStream in = process.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(in));
        } catch (IOException e) {
            System.out.println(Constants.ERROR+"Failed to open input stream to external process!");
            e.printStackTrace();
        }
    }

    public void produce() throws IOException {
        String line;
        while (runFlag && (line = bufferedReader.readLine()) != null ) {
            System.out.println(Constants.INFO+"Read new line: " + line);
            Transaction transaction = gson.fromJson(line, Transaction.class);
            while (dataQueue.isFull()) {
                try {
                    dataQueue.waitOnFull();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            if (!runFlag) {
                break;
            }
            dataQueue.add(transaction);
            dataQueue.notifyAllForEmpty();
            if(transaction.block_number >= config.endBlock){
                stop();
            }
        }
        System.out.println(Constants.INFO + "Producer Stopped");
    }

    public void stop() {
        runFlag = false;
        dataQueue.notifyAllForFull();
    }
}