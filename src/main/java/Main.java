import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        DataQueue queue = new DataQueue(10000000); //max block buffer size
        Producer producer = new Producer(queue);
        Executor pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()-2);
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors() -2 ; i++) {
            consumers.add(new Consumer(queue));
        }
        pool.execute(producer);
        consumers.forEach(consumer -> pool.execute(consumer));
    }
}
