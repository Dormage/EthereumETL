import java.util.Arrays;

public class ByteStructure {

    byte[] buffer; // last character of end offset is newline
    int startOffset; //first character of start offset is good [INCLUSIVE]
    int endOffset;

    int bufferId;

    public ByteStructure(byte[] buffer,long startOffset, long endOffset,int bufferId){
        //this.buffer = Arrays.copyOfRange(buffer,(int)startOffset,(int)endOffset);
        this.buffer = buffer;
        this.startOffset = (int) startOffset;
        this.endOffset = (int) endOffset;
        this.bufferId=bufferId;
    }

}
