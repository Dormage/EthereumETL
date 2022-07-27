import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {
        //delete leftover traces from ETL
        try {
            Files.delete(Paths.get("last_synced_block.txt"));
            System.out.println(Constants.SUCCESS + "Deleted previous files: ");
        } catch (IOException e) {
        }
        //load config
        Gson gson = new Gson();
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(Paths.get("config.json"));
        } catch (IOException e) {
            System.out.println(Constants.ERROR + "Missing config file: config.json");
            e.printStackTrace();
        }
        Config config = gson.fromJson(reader, Config.class);


        BlockingQueue<String> lineQueue = new LinkedBlockingQueue<String>();
        BlockingQueue<Transaction> insertionQueue = new LinkedBlockingQueue<Transaction>(100000);
        Status status = new Status(config, lineQueue, insertionQueue);
        AddressStore addressStore = new AddressStore(config);
        System.out.println(Constants.INFO + config);
        //ProducerManager producer = new ProducerManager(lineQueue, config, status, addressStore);
        Executor pool;
        if (config.readFile) {
            pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - config.producers - 2);
        } else {
            pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 2 - config.maxWorkers);
        }
        Executor producerPool = null;
        if (config.splitFile) {
            producerPool = Executors.newCachedThreadPool();
        }
        File files[] = null;
        if (config.splitFile) {
            Long startTime = System.currentTimeMillis();
            File dir = null;
            try {
                System.out.println(Constants.WARN + "Splitting file, this will take a while...");
                File root = new File(System.getProperty("user.dir"));
                dir = new File(root.getAbsolutePath() + File.separator + "cache");
                if (dir.mkdirs()) {
                    if (!root.isDirectory()) {
                        System.out.println(Constants.ERROR + "File directory not found!");
                    }
                    String command[] = new String[]{"split", "-d",
                            "-C", "" + config.splitSize,
                            "" + config.transactionsFile,
                            dir.getAbsolutePath() + File.separator + "part"};
                    ProcessBuilder builder = new ProcessBuilder(command);
                    Process process = builder.start();
                    process.waitFor();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(Constants.SUCCESS + "Splitting completed in " + (System.currentTimeMillis() - startTime) / 1000 + " seconds");
            files = dir.listFiles();
            System.out.println(Constants.INFO + "Input file split into " + files.length + "files");
        }


        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors() - config.producers - 2; i++) {
            consumers.add(new Consumer(lineQueue, insertionQueue, config, status, addressStore));
        }
        for (Consumer consumer : consumers) {
            pool.execute(consumer);
        }
        Timer timer = new Timer();
        timer.schedule(status, 0, 1000);
        Inserter dbFeeder = new Inserter(insertionQueue, config, status, addressStore);
        Thread thread = new Thread(dbFeeder);
        thread.start();
        if (config.splitFile) {
            CyclicBarrier cyclicBarrier = new CyclicBarrier(files.length, () -> {
                System.out.println("Completed level " + status.level + " waiting empty queue");
                while (!lineQueue.isEmpty()) {
                }
                System.out.println("Starting new level");
                status.level++;
                addressStore.createLevel();
                if (status.level == config.targetLevel) {
                    while (!lineQueue.isEmpty() || !insertionQueue.isEmpty() || !dbFeeder.fullBatch) {
                    }
                    dbFeeder.dumpBatch();
                    System.exit(0);
                }
            });


            for (int i = 0; i < files.length; i++) {
                producerPool.execute(new SplitProducer(lineQueue, config, status, addressStore, files[i], cyclicBarrier));
            }
        }
    }
}
