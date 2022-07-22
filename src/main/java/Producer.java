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
        try {
        ProcessBuilder builder = new ProcessBuilder("bash", "-i");
        builder.redirectErrorStream(true); // so we can ignore the error stream
        Process process = builder.start();
        InputStream in = process.getInputStream();
        bufferedReader = new BufferedReader(new InputStreamReader(in));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            produce();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void produce() throws IOException {
        String line;
        while (runFlag && (line = bufferedReader.readLine()) != null ) {
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
        }
        System.out.println("Producer Stopped");
    }

    public void stop() {
        runFlag = false;
        dataQueue.notifyAllForFull();
    }
}