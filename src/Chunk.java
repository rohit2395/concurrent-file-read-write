import java.nio.ByteBuffer;

public class Chunk {
    private ByteBuffer data;
    private int chunkNum;

    public Chunk(){
        this.data = null;
    }
    public Chunk(int chunkSize,int chunkNum) {
        this.data = ByteBuffer.allocate(chunkSize);
        this.data.clear();
        this.chunkNum = chunkNum;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = ByteBuffer.wrap(data);
    }

    public int getChunkNum() {
        return chunkNum;
    }

    public void setChunkNum(int chunkNum) {
        this.chunkNum = chunkNum;
    }
}
