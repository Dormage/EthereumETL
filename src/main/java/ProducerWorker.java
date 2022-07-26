import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class ProducerWorker implements Runnable{
    private final BlockingQueue<Transaction> queue;
    private boolean runFlag;
    private Config config;

    private static int idSequence = 0;

    private long currentBlock = 0;
    private Status status;

    private long fromByte;

    private long currentByte;
    private long toByte;

    private StringBuilder stringTransaction;

    private boolean isFirstLine = true;





    public ProducerWorker(BlockingQueue<Transaction> queue, Config config, Status status,long fromByte, long toByte) {
        this.queue = queue;
        this.config = config;
        runFlag = true;
        this.status = status;
        this.fromByte = fromByte;
        this.currentByte = fromByte;
        this.toByte = toByte;
    }


    @Override
    public void run() {
        try {
            FileChannel fileChannel  = new RandomAccessFile(config.transactionsFile, "r").getChannel();
            fileChannel.position(this.fromByte);
            stringTransaction = new StringBuilder();
            ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
            //CharsetDecoder charsetDecoder = Charset.forName("UTF-8").newDecoder();
            while (fileChannel.read(byteBuffer) > 0 && runFlag) {
                byteBuffer.flip();
                readBuffer(byteBuffer);
                byteBuffer.clear();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void readBuffer(ByteBuffer buffer){
        for (int i = 0; i < buffer.limit(); i++) {
            char c = (char) buffer.get();
            //System.out.print(c); //Print the content of file
            this.currentByte++;
            if ('\n' == c) {
                if(isFirstLine){ // throw away the first line, which is read by the previous thread or is a header
                    isFirstLine = false;
                }else{
                    enqueueTransaction(stringTransaction.toString());
                }
                stringTransaction = new StringBuilder();
                if(this.fromByte + this.currentByte > this.toByte){
                    //we reached the end
                    stop();
                    break;
                }
            }else{
                stringTransaction.append(c);
            }
        }
    }

    public void enqueueTransaction(String line){
        Transaction transaction = new Transaction(line.split(","));
        queue.offer(transaction);
        status.queueSize = queue.size();
        if (this.currentBlock < transaction.block_number) {
            if(this.currentBlock != 0){
                status.newBlock();
            }
            this.currentBlock = transaction.block_number;
        }
    }



    public void stop() {
        runFlag = false;
    }
}
