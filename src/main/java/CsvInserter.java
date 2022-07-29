import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import java.io.*;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class CsvInserter implements  Runnable{
    private final BlockingQueue<String[]> queue;
    private Config config;
    private Status status;
    private AddressStore addressStore;

    private List<Object[]> batch = new ArrayList<>();
    public int batchSize = 0;

    public FileOutputStream fileOutputStream = null;

    public CsvWriter writer;

    long writtenLines=0;

    public CsvInserter(BlockingQueue<String[]> queue, Config config, AddressStore addressStore) {
        this.queue = queue;
        this.config = config;
        batchSize = config.batchLimit;
        this.addressStore = addressStore;
        try {
            fileOutputStream = new FileOutputStream(new File(config.outputFile),true);
            Writer outputWriter = new OutputStreamWriter(fileOutputStream,"UTF-8");
            writer = new CsvWriter(outputWriter, new CsvWriterSettings());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void run(){
        feed();
    }

    public void feed(){
        while (true) {
            try {
                String[] trx = queue.take();
                if(batch.size()>batchSize){
                    //System.out.println("batch written "+ batch.size());
                    writeBatch();
                    batch.clear();
                }
                //System.out.print(".");
                batch.add(trx);
                writtenLines++;

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public void dumpBatch(){
        if(batch.size()>0) {
            writeBatch();
        }
        System.out.println(Constants.SUCCESS+ "Written " + writtenLines + " lines");
    }

    public void writeBatch(){
        try {
            writer.writeRows(this.batch);
        } catch (Exception e) {
            // handle exception
            System.out.println("An exception occurred when writing: "+ e);
        }
    }



}
