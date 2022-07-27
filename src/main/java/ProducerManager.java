import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

public class ProducerManager implements Runnable{

    private final BlockingQueue<ArrayList<ByteStructure>> queue;
    private volatile boolean runFlag;
    private Config config;
    private BufferedReader bufferedReader;
    private static int idSequence = 0;
    private Gson gson;
    private int currentBlock;
    private Status status;
    private AddressStore addressStore;
    private List<Transaction> buffer;

    int numWorkers = 0;

    public ProducerManager(BlockingQueue<ArrayList<ByteStructure>> queue, Config config, Status status, AddressStore addressStore) {
        this.queue = queue;
        this.config = config;
        runFlag = true;
        this.gson = new Gson();
        this.status = status;
        this.addressStore = addressStore;
        this.currentBlock = config.startBlock;
    }

    public void run(){
        long contentLength;
        try {
            RandomAccessFile f = new RandomAccessFile(config.transactionsFile, "r");
            contentLength = f.length();
            System.out.println("Total file size:" +contentLength);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.numWorkers = config.producers;
        List<Thread> workers = new ArrayList<>();
        while(status.level < config.targetLevel) {
            for (int i = 0; i < numWorkers; i++) {
                long[] range = getWorkRange(contentLength, i);
                workers.add(new Thread(new ProducerWorker(queue, config, status, range[0], range[1])));
            }
            for (Thread worker : workers) {
                worker.start();
            }
            for (Thread worker : workers) {
                try {
                    worker.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println(Constants.STATUS+" waiting for consumers, queue size: "+ queue.size());
            synchronized (status.monitor) {
                try {
                    status.monitor.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("notified!!!");
            workers.clear();
            addressStore.createLevel();
            status.level++;
            status.resetCurrentBlock();
            System.out.println(Constants.SUCCESS + "Total number of wallets added: " + addressStore.store.get(status.level).size());
        }
        System.out.println(Constants.SUCCESS + "Producer Manager completed!");

    }

    public long[] getWorkRange(long numBytes, int workerId){
        long[] range = new long[2];
        long bytesPerWorker = numBytes / this.numWorkers;
        range[0] = bytesPerWorker * workerId;
        if(range[0]==0){//do correction, to ensure that that also lines finishing exactly at bounds will be captured
            range[0] = 1;
        }
        range[1] = bytesPerWorker * (workerId+1);
        return range;
    }



}
