import com.google.gson.Gson;
import com.google.gson.internal.bind.util.ISO8601Utils;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        //delete lefover traces from ETL
        try {
            Files.delete(Paths.get("last_synced_block.txt"));
            System.out.println(Constants.SUCCESS + "Deleted previous files: " );
        } catch (IOException e) {
            e.printStackTrace();
        }

        Gson gson = new Gson();
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(Paths.get("config.json"));
        } catch (IOException e) {
            System.out.println(Constants.ERROR + "Missing config file: config.json");
            e.printStackTrace();
        }
        Config config = gson.fromJson(reader, Config.class);

        DataQueue queue = new DataQueue(10000000); //max block buffer size
        Producer producer = new Producer(queue, config);
        Executor pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 2);
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors() - 2; i++) {
            consumers.add(new Consumer(queue, config));
        }
        pool.execute(producer);
        consumers.forEach(consumer -> pool.execute(consumer));
    }
}
