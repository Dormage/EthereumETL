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
    static FileOutputStream fileOutputStream = null;
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

        try {
            fileOutputStream = new FileOutputStream(new File(config.outputFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i <config.targetLevel; i++) {
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
                    System.out.println(Constants.SUCCESS+ "Written " + writtenLines + " lines");
                    parsedRows.parallelStream().forEach(tokens -> {
                        Transaction transaction = new Transaction(tokens);
                        if (transaction.value.compareTo(new BigInteger("0")) != 0 && transaction.input.compareTo("0x") == 0) {
                            String lookup = String.valueOf(addressStore.contains(transaction.from_address)) + "--" + String.valueOf(addressStore.contains(transaction.to_address));
                            try {
                                switch (lookup) {
                                    case "true--false":
                                        if (addressStore.add(transaction.to_address)) {
                                        }
                                        writeTransaction(transaction);
                                        break;
                                    case "false--true":
                                        if (addressStore.add(transaction.from_address)) {
                                        }
                                        writeTransaction(transaction);
                                        break;
                                    case "true--true":
                                        writeTransaction(transaction);
                                        break;
                                }
                            } catch (Exception e) {
                                System.out.println("failed to insert in transaction queue: " + e);
                            }
                        }
                    });
                } catch (IOException e) {
                    // handle exception
                }
            }
        }
    }
    public static void writeTransaction( Transaction transactions){
        try (Writer outputWriter = new OutputStreamWriter(fileOutputStream,"UTF-8")){
            CsvWriter writer = new CsvWriter(outputWriter, new CsvWriterSettings());
            writer.writeRowsAndClose((Collection<Object[]>) transactions);
        } catch (IOException e) {
            // handle exception
        }
    }
}
