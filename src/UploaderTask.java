
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.Callable;

public class UploaderTask implements Callable<String> {

    private Chunk chunk;
    private String blobName;

    public UploaderTask(Chunk chunk, String blobName) {
        this.chunk = chunk;
        this.blobName = blobName;
    }

    @Override
    public String call() throws Exception {
        Boolean isDone = Boolean.FALSE;
        try {
            File chunkFile = new File("chunks/" +blobName+chunk.getChunkNum());
            if (chunkFile.createNewFile()) {
                FileOutputStream fo = new FileOutputStream(chunkFile);
                fo.write(chunk.getData().array());
            } else {
                System.out.println("chunkFile already exists.");
            }
            isDone = Boolean.TRUE;
        }catch (Exception e){
            System.out.println("Error while uploading chunk: "+chunk.getChunkNum());
            e.printStackTrace();
        }
        if (isDone)
            return chunk.getChunkNum()+"";
        else
            return null;
    }
}
