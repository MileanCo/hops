package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Random;

public class S3PerformanceTest {
    private AmazonS3 s3client;
    private S3AFileSystem s3afs;
    private String bucket;
    
    // test settings
    protected static final String ROOT_DIR = "/tmp/";
    private static final boolean verboseOption = false;
    private static final boolean positionReadOption = false;
    private String run_id;

    protected static Log LOG = LogFactory.getLog(S3PerformanceTest.class);
    private Random random = new Random();

    private MiniDFSCluster cluster;
    protected FileSystem mfs;
    

    @Before
    public void initJunitModeTest() throws Exception {
        LOG.info("initJunitModeTest");
    }

    //
    // hdfs tests
    //
//    @Test
//    public void testHDFSRecordTime100MB() throws IOException {
//        long BLOCK_SIZE = 1024 * 100000; // bytes
//        int WR_NTIMES = 30;
//        testHDFS(BLOCK_SIZE, WR_NTIMES);
//    }
    
    @Test
    public void testHDFSRecordTime10MB() throws IOException {
        long BLOCK_SIZE = 1024 * 10000; // bytes
        int WR_NTIMES = 30;
        testHDFS(BLOCK_SIZE, WR_NTIMES);
    }

    @Test
    public void testHDFSRecordTime100k() throws IOException {
        long BLOCK_SIZE = 1024 * 100; // bytes
        int WR_NTIMES = 30;
        testHDFS(BLOCK_SIZE, WR_NTIMES);
    }

    //
    // s3 tests
    //
//    @Test
//    public void testS3RecordTime100MB() throws IOException {
//        long BLOCK_SIZE = 1024 * 100000; // bytes
//        long BLOCK_SIZE_META = 8070000; // bytes
//        int WR_NTIMES = 30;
//        run_id = "/s3_perf_run" + random.nextInt(10000);
//        testS3(BLOCK_SIZE, BLOCK_SIZE_META, WR_NTIMES);
//    }
    
    @Test
    public void testS3RecordTime10MB() throws IOException {
        long BLOCK_SIZE = 1024 * 10000; // bytes
        long BLOCK_SIZE_META = 80700; // bytes
        int WR_NTIMES = 30;
        run_id = "/s3_perf_run" + random.nextInt(10000);

        testS3(BLOCK_SIZE, BLOCK_SIZE_META, WR_NTIMES);
    }

    @Test
    public void testS3RecordTime100K() throws IOException {
        long BLOCK_SIZE = 1024 * 100; // bytes
        long BLOCK_SIZE_META = 807; // bytes
        int WR_NTIMES = 30;
        run_id = "/s3_perf_run" + random.nextInt(10000);

        testS3(BLOCK_SIZE, BLOCK_SIZE_META, WR_NTIMES);
    }
    
    
    
    
    private void testHDFS(long blocksize, int wr_ntimes) throws IOException {
        Configuration conf = new HdfsConfiguration();
        // blocksize
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blocksize); // 100K
        conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

        // setup a cluster to run with
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        cluster.waitActive();
        mfs = cluster.getFileSystem();

        Path rootdir = new Path(ROOT_DIR);
        mfs.mkdirs(rootdir);
        
        long time = testWriteRead(mfs, run_id, false, blocksize, 0, wr_ntimes);
        double diffInSec = time / 1000.0;
        cluster.shutdown();

        LOG.info("\n\n\n-----------------------\n" +
                "It took " + diffInSec + " seconds to write " + wr_ntimes + " files with size " + blocksize +
                "\n---------------------------\n\n\n");
        
        cluster.shutdown();
    }
    
    private void testS3(long blocksize, long blockSizeMeta, int wr_ntimes) throws IOException {
        Configuration conf = new HdfsConfiguration();
        
        // set up a normal S3 file system
        this.s3afs = new S3AFileSystem();
        bucket = conf.get(DFSConfigKeys.S3_DATASET_BUCKET, "");
        URI rootURI = URI.create("s3a://" + bucket);
        s3afs.setWorkingDirectory(new Path("/"));
        s3afs.initialize(rootURI, conf);
        this.s3client = s3afs.getS3Client();

        // write/read block files
        long time1 = testWriteRead(s3afs, run_id, true, blocksize, blockSizeMeta, wr_ntimes);
        double diffInSec = time1 / 1000.0;
        LOG.info("In total=" + diffInSec + " seconds to write " + wr_ntimes + " files with size " + blocksize);
    }
    

    
    private long testWriteRead(FileSystem fs, String run_id, boolean is_s3, long blockSize, long blockSizeMeta, int num_files) {
        int countOfFailures = 0;
        byte[] outBuffer = new byte[(int)blockSize];
        byte[] inBuffer = new byte[(int)blockSize];

        byte[] outMetaBuffer = new byte[(int)blockSizeMeta];
        byte[] inMetaBuffer = new byte[(int)blockSizeMeta];
        
        for (int i = 0; i < blockSize; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }
        if (is_s3) {
            for (int i = 0; i < blockSizeMeta; i++) {
                outMetaBuffer[i] = (byte) (i & 0x00fa);
            }    
        }
        
        // counters
        long time_create = 0;
        int time_create_count = 0;
        
        long time_create_nn = 0;
        int time_create_nn_count = 0;
        
        long time_write = 0;
        int time_write_count = 0;
        
        long time_close = 0;
        int time_close_count = 0;
        
        Date beginTime = new Date();
        for (int i =0; i < num_files; i++) {
            String fname = run_id + "/block_" + i;
            String fname_meta = fname + ".meta";
            
            Path f = new Path(fname);
            try {                 
                if (is_s3) {
                    // simulate new block metadata...
                    Date start_create = new Date();
                    for (int j = 0; j < blockSizeMeta; j++) {
                        outMetaBuffer[j] = (byte) (j & 0x00fa);
                    }
                    long diffInMillies_create_meta = (new Date()).getTime() - start_create.getTime();
                    time_create += diffInMillies_create_meta;

                    Date start_create2 = new Date();
                    for (int j = 0; j < blockSize; j++) {
                        outBuffer[j] = (byte) (j & 0x00ff);
                    }
                    long diffInMillies_create = (new Date()).getTime() - start_create2.getTime();
                    time_create += diffInMillies_create;
                    time_create_count++;
                    
                    
                    // simulate getting both file's metadata once, 
                    try {
                        ObjectMetadata blockfile_meta = ((S3AFileSystem)fs).getObjectMetadata(new Path(fname));
                        Assert.fail("fname should have been empty, got: " + fname);
                    } catch (AmazonS3Exception err) {
                        Assert.assertTrue(err.getMessage().contains("404 Not Found"));
                    }
                    try {
                        ObjectMetadata blockmetafile_meta = ((S3AFileSystem)fs).getObjectMetadata(new Path(fname_meta));
                        Assert.fail("fname should have been empty, got: " + fname_meta);
                    } catch (AmazonS3Exception err) {
                        Assert.assertTrue(err.getMessage().contains("404 Not Found"));
                    }

                    //
                    // Write the block
                    //
//                    Date start_create = new Date();
//                    FSDataOutputStream out = fs.create(f);
//                    // counters
//                    long diffInMillies_create = (new Date()).getTime() - start_create.getTime();
//                    time_create += diffInMillies_create;
//                    time_create_count++;
//                    LOG.info("=== Client create file: " + diffInMillies_create + " ms to create new file & return output handle");
//
//                    Date start_write = new Date();
//                    writeData(out, outBuffer, (int) blockSize);
//                    long diffInMillies_write = (new Date()).getTime() - start_write.getTime();
//                    time_write += diffInMillies_write;
//                    time_write_count++;
//                    LOG.info("=== Client write file: " + diffInMillies_write + " ms to write all data to file");
//
//
//                    Date start_close = new Date();
//                    out.close();
//                    long diffInMillies_close = (new Date()).getTime() - start_close.getTime();
//                    time_close += diffInMillies_close;
//                    time_close_count++;
//                    LOG.info("=== Client close file: " + diffInMillies_close + " ms to upload file to s3" );
                    //
                    // END WRITE BLOCK
                    //
//                    FSDataOutputStream out_meta = fs.create(new Path(fname_meta));
//                    writeData(out_meta, outMetaBuffer, (int) blockSizeMeta);
//                    out_meta.close();

                    
                    // WRITE meta file
                    Date start_write_meta2 = new Date();
                    File local_meta_file  = new File("tmp" + fname_meta);
                    FileUtils.writeByteArrayToFile(local_meta_file, outMetaBuffer);
                    long diffInMillies_write_meta2 = (new Date()).getTime() - start_write_meta2.getTime();
                    time_write += diffInMillies_write_meta2;

                    // WRITE BLOCK FILE
                    Date start_write = new Date();
                    File local_block_file  = new File("tmp" + fname);
                    FileUtils.writeByteArrayToFile(local_block_file, outBuffer);
                    long diffInMillies_write = (new Date()).getTime() - start_write.getTime();
                    time_write += diffInMillies_write;
                    time_write_count++; // only once


                    // CLOSE BLOCK
                    Date start_close = new Date();
                    PutObjectRequest putReqBlock = new PutObjectRequest(bucket, fname.substring(1), local_block_file);
                    PutObjectResult putResult = s3client.putObject(putReqBlock);
                    local_block_file.delete();
                    long diffInMillies_close = (new Date()).getTime() - start_close.getTime();
                    time_close += diffInMillies_close;
                    time_close_count++; // only do this once

                    // close meta block
                    Date start_close_meta = new Date();
                    PutObjectRequest putReqMeta = new PutObjectRequest(bucket, fname_meta.substring(1), local_meta_file);
                    PutObjectResult putMetaResult = s3client.putObject(putReqMeta);
                    local_meta_file.delete();
                    long diffInMillies_close_meta = (new Date()).getTime() - start_close_meta.getTime();
                    time_close += diffInMillies_close_meta;
                    
//                    time_close_count++; // dont increase close count b/c this time is counted together with block time

//                    File dest1 = new File(local_block_file + ".downloaded");
//                    File dest2 = new File(local_meta_file + ".downloaded");
//                    FileUtils.copyInputStreamToFile(s3afs.open(new Path(fname)).getWrappedStream(), dest1);
//                    FileUtils.copyInputStreamToFile(s3afs.open(new Path(fname_meta)).getWrappedStream(), dest2);

                    // reads this from S3 now
                    readData(fname, inBuffer, outBuffer.length, 0, fs, blockSize);
                    readData(fname_meta, inMetaBuffer, outMetaBuffer.length, 0, fs, blockSizeMeta);
                    
//                    local_block_file.delete();
//                    local_meta_file.delete();
//                    dest1.delete();
//                    dest2.delete();
//                    
                } else {
                    
                    Date start_create = new Date();
                    // WRite the file
                    FSDataOutputStream out = fs.create(f);
                    // counters
                    long diffInMillies_create = (new Date()).getTime() - start_create.getTime();
                    time_create += diffInMillies_create;
                    time_create_count++;
                    LOG.info("=== Client create file: " + diffInMillies_create + " ms to create new file & return output handle");

                    Date start_write = new Date();
                    writeData(out, outBuffer, (int) blockSize);
                    long diffInMillies_write = (new Date()).getTime() - start_write.getTime();
                    time_write += diffInMillies_write;
                    time_write_count++;
                    LOG.info("=== Client write file: " + diffInMillies_write + " ms to write all data to file");

                    Date start_close = new Date();
                    out.close();
                    long diffInMillies_close = (new Date()).getTime() - start_close.getTime();
                    time_close += diffInMillies_close;
                    time_close_count++;
                    LOG.info("=== Client close file: " + diffInMillies_close + " ms to upload file to s3" );


//                    File dest1  = new File("tmp" + fname + ".downloaded");
//                    FileUtils.copyInputStreamToFile(fs.open(new Path(fname)).getWrappedStream(), dest1);
                    
                    // Read the file
                    long byteVisibleToRead = readData(fname, inBuffer, outBuffer.length, 0, fs, blockSize);
                    String readmsg =
                            "Written=" + outBuffer.length + " ; Expected Visible=" +
                                    outBuffer.length + " ; Got Visible=" + byteVisibleToRead +
                                    " of file " + fname;

                    if (verboseOption) {
                        LOG.info(readmsg);
                    }
                    
                }
//                if (byteVisibleToRead >= totalByteVisible && byteVisibleToRead <= totalByteWritten) {
//                    readmsg =
//                            "pass: reader sees expected number of visible byte. " + readmsg +
//                                    " [pass]";
//                } else {
//                    countOfFailures++;
//                    readmsg =
//                            "fail: reader see different number of visible byte. " + readmsg +
//                                    " [fail]";
//                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Date endTime = new Date();
        Assert.assertEquals(0, countOfFailures);
        
        // counters
        double time_create_avg = time_create / time_create_count;
        double time_write_avg = time_write / time_write_count;
        double time_close_avg = time_close / time_close_count;
        
        LOG.info("------------------ \n");
        LOG.info("Create avg time: " + time_create_avg);
        LOG.info("Write avg time: " + time_write_avg);
        LOG.info("Close avg time: " + time_close_avg);
        LOG.info("------------------ ");
        LOG.info("Create total time: " + time_create);
        LOG.info("Write total time: " + time_write);
        LOG.info("Close total time: " + time_close);
        LOG.info("------------------ \n");
        
        long diffInMillies = endTime.getTime() - beginTime.getTime();
        return diffInMillies;
    }

    // copied from TestWriteRead
    private void writeData(FSDataOutputStream out, byte[] buffer, int length)
            throws IOException {

        int totalByteWritten = 0;
        int remainToWrite = length;

        while (remainToWrite > 0) {
            int toWriteThisRound =
                    remainToWrite > buffer.length ? buffer.length : remainToWrite;
            
            out.write(buffer, 0, toWriteThisRound);
            totalByteWritten += toWriteThisRound;
            remainToWrite -= toWriteThisRound;
        }
        if (totalByteWritten != length) {
            throw new IOException(
                    "WriteData: failure in write. Attempt to write " + length +
                            " ; written=" + totalByteWritten);
        }
    }

    /**
     * Open the file to read from begin to end. Then close the file.
     * Return number of bytes read.
     * Support both sequential read and position read.
     */
    // copied from TestWriteRead
    private long readData(String fname, byte[] buffer, long byteExpected, long beginPosition, FileSystem fs, long blockSize) throws IOException {
        long totalByteRead = 0;
        Path path = fs.makeQualified(new Path(fname));

        FSDataInputStream in = null;
        try {
            in = fs.open(path);

            long visibleLenFromReadStream = blockSize;
//                    ((HdfsDataInputStream) in).getVisibleLength();

            if (visibleLenFromReadStream < byteExpected) {
                throw new IOException(visibleLenFromReadStream +
                        " = visibleLenFromReadStream < bytesExpected= " + byteExpected);
            }

            totalByteRead =
                    readUntilEnd(in, buffer, buffer.length, fname, beginPosition,
                            visibleLenFromReadStream, positionReadOption);
            in.close();

            // reading more data than visibleLeng is OK, but not less
            if (totalByteRead + beginPosition < byteExpected) {
                throw new IOException(
                        "readData mismatch in byte read: expected=" + byteExpected +
                                " ; got " + (totalByteRead + beginPosition));
            }
            return totalByteRead + beginPosition;

        } catch (IOException e) {
            throw new IOException(
                    "##### Caught Exception in readData. " + "Total Byte Read so far = " +
                            totalByteRead + " beginPosition = " + beginPosition, e);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    /**
     * read chunks into buffer repeatedly until total of VisibleLen byte are
     * read.
     * Return total number of bytes read
     */
    private long readUntilEnd(FSDataInputStream in, byte[] buffer, long size,
                              String fname, long pos, long visibleLen, boolean positionReadOption)
            throws IOException {

        if (pos >= visibleLen || visibleLen <= 0) {
            return 0;
        }

        int chunkNumber = 0;
        long totalByteRead = 0;
        long currentPosition = pos;
        int byteRead = 0;
        long byteLeftToRead = visibleLen - pos;
        int byteToReadThisRound = 0;

        if (!positionReadOption) {
            in.seek(pos);
            currentPosition = in.getPos();
        }
        if (verboseOption) {
            LOG.info("reader begin: position: " + pos + " ; currentOffset = " +
                    currentPosition + " ; bufferSize =" + buffer.length +
                    " ; Filename = " + fname);
        }
        try {
            while (byteLeftToRead > 0 && currentPosition < visibleLen) {
                byteToReadThisRound =
                        (int) (byteLeftToRead >= buffer.length ? buffer.length :
                                byteLeftToRead);
                if (positionReadOption) {
                    byteRead = in.read(currentPosition, buffer, 0, byteToReadThisRound);
                } else {
                    byteRead = in.read(buffer, 0, byteToReadThisRound);
                }
                if (byteRead <= 0) {
                    break;
                }
                chunkNumber++;
                totalByteRead += byteRead;
                currentPosition += byteRead;
                byteLeftToRead -= byteRead;

                if (verboseOption) {
                    LOG.info("reader: Number of byte read: " + byteRead +
                            " ; totalByteRead = " + totalByteRead + " ; currentPosition=" +
                            currentPosition + " ; chunkNumber =" + chunkNumber +
                            "; File name = " + fname);
                }
            }
        } catch (IOException e) {
            throw new IOException(
                    "#### Exception caught in readUntilEnd: reader  currentOffset = " +
                            currentPosition + " ; totalByteRead =" + totalByteRead +
                            " ; latest byteRead = " + byteRead + "; visibleLen= " +
                            visibleLen + " ; bufferLen = " + buffer.length +
                            " ; Filename = " + fname, e);
        }

        if (verboseOption) {
            LOG.info("reader end:   position: " + pos + " ; currentOffset = " +
                    currentPosition + " ; totalByteRead =" + totalByteRead +
                    " ; Filename = " + fname);
        }

        return totalByteRead;
    }
    
        
    


    /**
     * 
     */
//    public static void main(String[] args) {
//        try {
//            S3PerformanceTest test = new S3PerformanceTest();
//
//            System.exit(0);
//        } catch (IOException e) {
////            LOG.info("#### Exception in Main");
//            e.printStackTrace();
//            System.exit(-2);
//        }
//    }
}
