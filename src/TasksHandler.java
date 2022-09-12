
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.domain.Blob;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TasksHandler {

    private final String fileName;
    private final String restoreFileName;
    private final long fileSize;
    private final int chunkSize;

    private final String blobName;


    private final ListeningExecutorService readerTasksExecutor;
    private final ListeningExecutorService uploaderTasksExecutor;

    private final ListeningExecutorService downloaderTasksExecutor;
    private final ListeningExecutorService writerTasksExecutor;

    public TasksHandler(String fileName, long fileSize, int chunkSize, String blobName, int threads) {
        this.fileName = fileName;
        this.restoreFileName = fileName+"_restored";
        this.fileSize = fileSize;
        this.chunkSize = chunkSize;
        this.blobName = blobName;

        readerTasksExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads));
        uploaderTasksExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads));

        downloaderTasksExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads));
        writerTasksExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads));
    }

    public String getFileName() {
        return fileName;
    }

    public String getRestoreFileName() {
        return restoreFileName;
    }

    /*
            Multithreaded operations for chunk read, chunk upload , chunk download and chunk write
        */
    public int performReadAndUpload(){
        long startTime = System.currentTimeMillis();

        int c = (int) performRead();

        try{
            readerTasksExecutor.shutdown();
            if(readerTasksExecutor.awaitTermination(15, TimeUnit.MINUTES)){
                System.out.println("All reader threads successfully executed");
            }else{
                System.out.println("Reader executor timeout occurred!");
            }

            uploaderTasksExecutor.shutdown();
            if(uploaderTasksExecutor.awaitTermination(15,TimeUnit.MINUTES)){
                System.out.println("All chunks successfully uploaded");
            }else{
                System.out.println("Uploader Executor timeout occurred!");
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Total time taken for upload: "+(endTime - startTime));
        return  c;
    }

    public void performDownloadAndWrite(int c) throws IOException {
        long startTime = System.currentTimeMillis();
        System.out.println("Performing restore");
        performDownload(c);

        try{
            downloaderTasksExecutor.shutdown();
            if(downloaderTasksExecutor.awaitTermination(15, TimeUnit.MINUTES)){
                System.out.println("All downloader threads successfully executed");
            }else{
                System.out.println("Download executor timeout occurred!");
            }

            writerTasksExecutor.shutdown();
            if(writerTasksExecutor.awaitTermination(15,TimeUnit.MINUTES)){
                System.out.println("All chunks successfully written");
            }else{
                System.out.println("Writer Executor timeout occurred!");
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Total time taken for download: "+(endTime - startTime));
    }

    // Perform file read in chunks concurrently
    private long performRead() {
        File file = new File(fileName);


        long c = 1;
        try {

            for(long remBytes = fileSize; remBytes > 0 ;){
                FileChannel fc = new FileInputStream(file).getChannel().position(chunkSize * (c-1));
                remBytes = (fileSize - fc.position());
                long bytesToRead = Math.min(remBytes, chunkSize);
                if(bytesToRead > 0) {
                    ReaderTask task = new ReaderTask(fc,bytesToRead,(int)c);
                    final ListenableFuture<Chunk> fut = readerTasksExecutor.submit(task);
                    //Callback method for the submitted tasks
                    fut.addListener(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Chunk chunk = fut.get();
                                System.out.println("Chunk "+chunk.getChunkNum()+" was read successfully at: "+System.currentTimeMillis());
                                performUpload(chunk);
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                    }, MoreExecutors.sameThreadExecutor());
                }
                c++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return c;
    }

    // Perform upload task for the read chunk by reader thread
    private void performUpload(Chunk chunk){
        UploaderTask task = new UploaderTask(chunk,blobName);
        final ListenableFuture<String> fut = uploaderTasksExecutor.submit(task);
        //Callback method for the submitted tasks
        fut.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    String chunkName = fut.get();
                    if (chunkName != null)
                        System.out.println("Chunk "+chunkName+" uploaded to object store successfully at: "+System.currentTimeMillis());
                    else
                        System.out.println("Failed to upload a chunk on the object store");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        },MoreExecutors.sameThreadExecutor());
    }

    // Perform blob download concurrently
    private void performDownload(int c) throws IOException {


        //crfeate restored file
        File res = new File(restoreFileName);
        res.createNewFile();

        System.out.println("Total number of chunks: "+c);
        for(int i=1;i<=c;i++){
            File file = new File("chunks/"+blobName+i);
            FileChannel fc = new FileInputStream(file).getChannel().position(0);
            DownloaderTask task = new DownloaderTask(fc,"chunks/"+blobName+i,i);
            final ListenableFuture<Chunk> fut = downloaderTasksExecutor.submit(task);
            //Callback method for the submitted tasks
            fut.addListener(new Runnable() {
                @Override
                public void run() {
                    try {
                        Chunk chunk = fut.get();
                        if(chunk != null){
                            System.out.println("Chunk "+chunk.getChunkNum()+" was downloaded successfully at: "+System.currentTimeMillis());
                            performWrite(chunk);
                        }else{
                            System.err.println("Chunk download failed at: "+System.currentTimeMillis());
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            },MoreExecutors.sameThreadExecutor());
        }

    }

    // Perform chunk write task
    private void performWrite(Chunk chunk){
        try {
            File file = new File(restoreFileName);
            System.out.println("Writing data at: "+(chunk.getChunkNum()-1));
//            FileChannel fc = new FileOutputStream(file,true).getChannel().position((long) chunkSize * (chunk.getChunkNum() - 1));
            FileChannel fc = new FileOutputStream(file,true).getChannel();
            WriterTask task = new WriterTask(fc,chunk,(long) chunkSize * (chunk.getChunkNum() - 1));
            final ListenableFuture<Integer> fut = writerTasksExecutor.submit(task);
            //Callback method for the submitted tasks
            fut.addListener(new Runnable() {
                @Override
                public void run() {
                    try {
                        Integer chunkNum = fut.get();
                        if (chunkNum != null)
                            System.out.println("Chunk "+chunkNum+" written to restore file successfully at: "+System.currentTimeMillis());
                        else
                            System.out.println("Failed to write a chunk on the restore file");
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            },MoreExecutors.sameThreadExecutor());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
