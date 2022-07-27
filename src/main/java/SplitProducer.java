import com.google.gson.Gson;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class SplitProducer implements Runnable {
    private final BlockingQueue<String> queue;
    private volatile boolean runFlag;
    private Config config;
    private BufferedReader bufferedReader;
    private static int idSequence = 0;
    private Gson gson;
    private int currentBlock;
    private Status status;
    private AddressStore addressStore;
    private File sourceFile;
    private CyclicBarrier cyclicBarrier;



    public SplitProducer(BlockingQueue<String> queue, Config config, Status status, AddressStore addressStore, File sourceFile, CyclicBarrier cyclicBarrier) {
        this.queue = queue;
        this.config = config;
        runFlag = true;
        this.gson = new Gson();
        this.currentBlock = config.startBlock;
        this.status = status;
        this.addressStore = addressStore;
        this.currentBlock = config.startBlock;
        this.sourceFile = sourceFile;
        this.cyclicBarrier = cyclicBarrier;
    }

    @Override
    public void run() {

        try {
            while (runFlag) {
                bufferedReader = new BufferedReader(new FileReader(sourceFile));
                String line;

                //pay attention to the csv header
                line = bufferedReader.readLine();
                if(!line.contains("nonce")){
                    queue.offer(line);
                    status.newTransaction();
                }

                while ((line = bufferedReader.readLine()) != null) {
                    queue.offer(line);
                    status.newTransaction();
                }
                //System.out.println(Constants.STATUS+"Consumer " + Thread.currentThread().getName() + " finished reading " + sourceFile.getName() + " file!");
                cyclicBarrier.await();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        runFlag = false;
    }
}