import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

public class ProducerWorker implements Runnable{
    private final BlockingQueue<ArrayList<ByteStructure>> queue;
    private boolean runFlag;
    private Config config;

    private static int idSequence = 0;

    private long currentBlock = 0;
    private Status status;

    private long fromByte;

    private long currentByte;
    private long toByte;



    private boolean isFirstLine = true;

    private ArrayList<ByteStructure> bufferOfBuffers;

    int dumpThreshold;// = 10; //number of buffers

    RandomAccessFile rf;

    int bufferLen;// = 100000;//1048567;

    int bufferid = 0;




    public ProducerWorker(BlockingQueue<ArrayList<ByteStructure>> queue, Config config, Status status, long fromByte, long toByte) {
        this.queue = queue;
        this.config = config;
        runFlag = true;
        this.status = status;
        this.fromByte = fromByte;
        this.currentByte = fromByte;
        this.toByte = toByte;
        this.bufferOfBuffers = new ArrayList<ByteStructure>(dumpThreshold+1);
        this.dumpThreshold = config.dumpThreshold;
        this.bufferLen = config.bufferLen;
    }


    @Override
    public void run() {
        try {
            rf  = new RandomAccessFile(config.transactionsFile, "r");
            rf.seek(this.fromByte);
            System.out.println("start reading from: "+fromByte + " to byte: "+toByte);
            byte[] buffer = new byte[bufferLen];
            //CharsetDecoder charsetDecoder = Charset.forName("UTF-8").newDecoder();
            long read = rf.read(buffer);
            while (rf.getFilePointer() < toByte && runFlag) {
                cutAndInsert(buffer,read);
                buffer = new byte[bufferLen];
                read = rf.read(buffer);
            }
            stop();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void cutAndInsert(byte[] buffer,long bytesRead){
        bytesRead--;
        long startOffset = 0;
        long endOffset = 0;
        if(isFirstLine){
            isFirstLine = false;
            char lastChar;
            int i = 0;
            do{
                lastChar = (char) buffer[i];
                i++;
            }while(lastChar!='\n'&&i<bytesRead);
            startOffset = i;
        }
        char lastChar;
        int i = (int) bytesRead;
        do{
            lastChar = (char) buffer[i-1];
            i--;
        }while(lastChar!='\n'&&i>0);
        endOffset = i+1;
        try {
            //System.out.println("pointer: "+rf.getFilePointer());
            rf.seek(rf.getFilePointer()-(bytesRead-endOffset));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //System.out.println("offests: "+startOffset+" e: "+endOffset);
        addBuffer(buffer,startOffset,endOffset);
        /*for (int i = 0 ; i < 100;i++){
            System.out.print((char)buffer[i]);
        }
        System.out.println();*/

    }

    public void addBuffer(byte[] buffer,long startOffset, long endOffset) {
        bufferOfBuffers.add(new ByteStructure(buffer,startOffset,endOffset,bufferid));
        if(bufferOfBuffers.size()>dumpThreshold){
            //dump the buffer
            queue.offer(bufferOfBuffers);
            bufferOfBuffers = new ArrayList<ByteStructure>(dumpThreshold+1);
        }
    }



    public void stop() {
        runFlag = false;
        if(bufferOfBuffers.size()>0){
            queue.offer(bufferOfBuffers);
            bufferOfBuffers = new ArrayList<ByteStructure>(dumpThreshold+1);
        }
    }
}
