import com.google.gson.Gson;
import com.univocity.parsers.common.processor.BatchedColumnProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import javax.swing.*;
import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.*;

public class Main {
    public static int writtenLines=0;

    public static CsvInserter inserter;

    public static BlockingQueue<String[]> queue = new LinkedBlockingQueue<String[]>();
    
    public static int currentLevel = 0;

    public static long insertion_count = 0;
    public static long trx_count = 0;

    //only for debugging
    public static synchronized void  offerInsert(){
        insertion_count++;
    }
    //only for debugging
    public static synchronized void  countTrx(){
        trx_count++;
    }

    public static void main(String[] args) {
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
        try {
            Files.delete(Paths.get(config.outputFile));
            System.out.println(Constants.SUCCESS + "Deleted previous files: ");
        } catch (IOException e) {
        }
        AddressStore addressStore = new AddressStore(config);
        System.out.println(Constants.INFO + config);

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



        //create the CSV writer
        inserter = new CsvInserter(queue,config,addressStore);

        Thread thread = new Thread(inserter);
        thread.start();

        for (int i = 0; i <config.targetLevel; i++) {
            long level_time = System.currentTimeMillis();
            for (int j = 0; j < files.length; j++) {
                System.out.println(Constants.SUCCESS + "Opening new file: " + files[j].getName());
                try (Reader inputReader = new InputStreamReader(new FileInputStream(files[j]), "UTF-8")) {
                    CsvParserSettings settings = new CsvParserSettings();
                    settings.setProcessor(new BatchedColumnProcessor(100) {
                        @Override
                        public void batchProcessed(int rowsInThisBatch) {
                        }
                    });
                    settings.setMaxCharsPerColumn(4097 * 100);
                    CsvParser parser = new CsvParser(settings);
                    long time = System.currentTimeMillis();
                    List<String[]> parsedRows = parser.parseAll(inputReader);
                    System.out.println(Constants.STATUS + "Read: " + parsedRows.size() + " rows in: " + (System.currentTimeMillis() - time) / 1000 + " seconds" );
                    time = System.currentTimeMillis();
                    parsedRows.parallelStream().forEach(token -> {
                        String hash = token[0], from = token[5], to = token[6], value = token[7], input = token[10];
                        if (value.compareTo("0") != 0 && input.compareTo("0x") == 0) {
                            String lookup = String.valueOf(addressStore.contains(from)) + "--" + String.valueOf(addressStore.contains(to));
                            try {
                                switch (lookup) {
                                    case "true--false":
                                        addressStore.add(to);
                                        if(!addressStore.isAlreadyWritten(hash)){
                                            token[13] = Integer.toString(currentLevel);// change to current level
                                            queue.offer(token);
                                        }
                                        break;
                                    case "false--true":
                                        addressStore.add(from);
                                        if(!addressStore.isAlreadyWritten(hash)){
                                            token[13] = Integer.toString(currentLevel);// change to current level
                                            queue.offer(token);
                                        }
                                        break;
                                    case "true--true":
                                        if(!addressStore.isAlreadyWritten(hash)){
                                            token[13] = Integer.toString(currentLevel);// change to current level
                                            queue.offer(token);
                                        }
                                        break;
                                }
                            } catch (Exception e) {
                                System.out.println("failed to insert in transaction:" + e);
                                System.out.println("Queueu size: "+ queue.size());
                            }
                        }
                    });
                    System.out.println(Constants.STATUS + "Processed in: " + (System.currentTimeMillis() - time) / 1000 + " seconds" );
                } catch (IOException e) {
                    // handle exception
                    System.out.println("Exception when reading files: "+ e);
                }
            }
            System.out.println(Constants.STATUS+ "Level "+currentLevel+ " completed in: " + (System.currentTimeMillis() - level_time) / 1000 + " seconds" +" total written lines: "+ inserter.writtenLines);
            currentLevel++; //increase level
            addressStore.createLevel();
        }
        //System.out.println("offered: "+insertion_count + " all trx: "+trx_count);
        while(!queue.isEmpty()){//busy waiting
        }
        inserter.dumpBatch();
        System.exit(0);

    }

}
