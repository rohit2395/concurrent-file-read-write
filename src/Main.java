import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {

    public static long getFileSize(String fileName) {
        Path path = Paths.get(fileName);
        long bytes = 0;
        try {
            bytes = Files.size(path);
            System.out.println(String.format("%,d bytes", bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }


    public static void sequentialRead(String fileName,long fileSize,int chunkSize){
        long startTime = System.currentTimeMillis();
        File file = new File(fileName);
        try {

            long bytesToRead = 0;
            long c = 1;
            for(long remBytes = fileSize; remBytes > 0 ;){
                FileInputStream in = new FileInputStream(file);
                remBytes = (fileSize - in.skip(chunkSize * (c-1)));
                bytesToRead = Math.min(remBytes, chunkSize);
                System.out.println("Bytes to read: "+bytesToRead);
                if(bytesToRead > 0) {
                    Chunk chunk = new Chunk((int)bytesToRead,(int)c);

//                    in.read(chunk.getData(), 0,(int)bytesToRead);

                    File chunkFile = new File("chunks/chunk_" + c);
                    if (chunkFile.createNewFile()) {
                        FileOutputStream fo = new FileOutputStream(chunkFile);
//                        fo.write(chunk.getData());
                    } else {
                        System.out.println("chunkFile already exists.");
                    }
                }
                c++;
            }

            long endTime = System.currentTimeMillis();
            System.out.println("Time taken: "+(endTime - startTime));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {

        String fileName = "rohit.txt";
        long fileSize = getFileSize(fileName);
        int chunkSize = 128 * 1024 * 1024;
        String blobName = "chunk_";

//        TasksHandler tasksHandler = new TasksHandler(fileName,fileSize,chunkSize,blobName,10);
//        int c = tasksHandler.performReadAndUpload();
//        tasksHandler.performDownloadAndWrite(c-2);

        File file = new File(fileName);
        long pos = 1;
        FileChannel fc = new FileOutputStream(file,true).getChannel();
        String data = "\nTis is new line :"+pos;
        System.out.println(data.getBytes().length);
        fc.write(ByteBuffer.wrap(data.getBytes()),pos * data.getBytes().length);


    }
}
