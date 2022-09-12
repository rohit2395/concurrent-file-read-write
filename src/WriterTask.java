
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;

public class WriterTask implements Callable<Integer> {

    private final FileChannel pointer;
    private final Chunk chunk;
    private final long pos;

    public WriterTask(FileChannel pointer, Chunk chunk, long pos) {
        this.pointer = pointer;
        this.chunk = chunk;
        this.pos = pos;
    }

    @Override
    public Integer call() throws Exception {
        Boolean isDone = Boolean.FALSE;
        try {
            System.out.println("Data size: "+chunk.getData().array().length);
            pointer.write(chunk.getData(),pos);
            isDone = Boolean.TRUE;
        }catch (Exception e){
            System.out.println("Error while writing chunk: "+chunk.getChunkNum());
            e.printStackTrace();
        }
        if (isDone)
            return chunk.getChunkNum();
        else
            return null;
    }

}
