import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

public class DownloaderTask implements Callable<Chunk> {

    private FileChannel pointer;
    private String blobName;
    private int chunkNum;

    public DownloaderTask(FileChannel pointer, String blobName, int chunkNum) {
        this.pointer = pointer;
        this.blobName = blobName;
        this.chunkNum = chunkNum;
    }

    @Override
    public Chunk call() throws Exception {

        Path path = Paths.get(blobName);
        long bytes = 0;
        try {
            bytes = Files.size(path);
            System.out.println(String.format("File "+blobName+" size: %,d KB", bytes / 1024));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Chunk chunk = new Chunk();
        try {
            ByteBuffer data = ByteBuffer.allocate((int)bytes);
            data.clear();
            pointer.read(data);
            chunk.setChunkNum(chunkNum);
            chunk.setData(data.array());
        }catch (Exception e){
            System.out.println("Error while creating chunk: "+ chunkNum);
            e.printStackTrace();
        }
        return chunk;
    }

}
