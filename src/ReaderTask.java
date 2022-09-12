
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;

public class ReaderTask implements Callable<Chunk> {

    private final FileChannel pointer;
    private final long chunkSize;
    private final int chunkNum;

    public ReaderTask(FileChannel pointer, long chunkSize, int chunkNum) {
        this.pointer = pointer;
        this.chunkSize = chunkSize;
        this.chunkNum = chunkNum;
    }

    @Override
    public Chunk call() throws Exception {
        Chunk chunk = null;
        try {
            chunk = new Chunk((int) chunkSize,chunkNum);
            pointer.read(chunk.getData());
        }catch (Exception e){
            System.out.println("Error while creating chunk: "+chunkNum);
            e.printStackTrace();
        }
        return chunk;
    }
}
