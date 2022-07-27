import com.google.gson.Gson;

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
        } catch (IOException e) {}
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


        BlockingQueue<ArrayList<ByteStructure>> queue = new LinkedBlockingQueue<>(100000);
        Status status = new Status(config,queue);
        AddressStore addressStore = new AddressStore(config);
        System.out.println(Constants.INFO + config);
        ProducerManager producer = new ProducerManager(queue, config, status, addressStore);
        Executor pool;
        if(config.readFile){
            pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - config.producers );
        }else{
            pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 2 - config.maxWorkers);
        }
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors() - config.producers - 2; i++) {
            consumers.add(new Consumer(queue, config, status, addressStore));
        }
        pool.execute(producer);
        consumers.forEach(consumer -> pool.execute(consumer));
        Timer timer = new Timer();
        timer.schedule(status, 0, 1000);
    }
}
