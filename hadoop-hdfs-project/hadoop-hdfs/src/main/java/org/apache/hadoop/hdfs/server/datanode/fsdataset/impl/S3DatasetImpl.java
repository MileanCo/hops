package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystemCommon;
import org.apache.hadoop.fs.s3a.UploadInfo;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.ReflectionUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;

import static org.apache.hadoop.hdfs.protocol.Block.BLOCK_FILE_PREFIX;
import static org.apache.hadoop.hdfs.protocol.Block.METADATA_EXTENSION;

public class S3DatasetImpl extends FsDatasetImpl {
    private String bucket;
    private S3AFileSystemCommon s3afs;
    private DatanodeProtocolClientSideTranslatorPB namenode;
    
    // Map of block pool Id to another map of block Id to S3FinalizedReplica
    private Map<String, Map<Long, S3FinalizedReplica>> downloadedBlocksMap = new HashMap<>();
    
    /**
     * An FSDataset has a directory where it loads its data files.
     *
     * @param datanode
     * @param storage
     * @param conf
     */ 
    
    S3DatasetImpl(DataNode datanode, DataStorage storage, Configuration conf) throws IOException {
        super(datanode, storage, conf);
        // create new voluemMap that checks S3 if block is not found locally
        super.volumeMap = new S3ReplicaMap(this, this);
        bucket = conf.get(DFSConfigKeys.S3_DATASET_BUCKET, "");
        
        Class<? extends S3AFileSystemCommon> s3AFilesystemClass = conf.getClass(
                DFSConfigKeys.S3A_IMPL, S3AFileSystemCommon.class,
                S3AFileSystemCommon.class);
        
        s3afs = ReflectionUtils.newInstance(s3AFilesystemClass, conf);
        URI rootURI = URI.create("s3a://" + bucket);
        
        s3afs.initialize(rootURI, conf);
        s3afs.setWorkingDirectory(new Path("/"));
        
        // also connect to NN
        List<InetSocketAddress> addrs = DFSUtil.getNameNodesServiceRpcAddresses(conf);
        namenode = datanode.connectToNN(addrs.get(0));
        // TODO: use below instead whenver a block is queried?
        //namenode = datanode.getActiveNamenodeForBP();
    }

    @Override // FsDatasetSpi
    public synchronized ReplicaInPipeline createRbw(StorageType storageType,
                                                    ExtendedBlock b) throws IOException {
        // will just overwrite instead
        // checks local filesystem and S3 for the block
//        ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
//        if (replicaInfo != null) {
//            throw new ReplicaAlreadyExistsException("Block " + b +
//                    " already exists in state " + replicaInfo.getState() +
//                    " and thus cannot be created.");
//        }
        
        // create a new block
        FsVolumeImpl v = volumes.getNextVolume(storageType, b.getNumBytes());

        // create an rbw file to hold block in the designated volume
        File f = v.createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
        ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(b.getBlockId(),
                b.getGenerationStamp(), v, f.getParentFile(), b.getNumBytes());
        volumeMap.add(b.getBlockPoolId(), newReplicaInfo);

        return newReplicaInfo;
    }
    
    public void downloadS3BlockTo(ExtendedBlock b, File dest) throws IOException {
        // TODO: check cache if block exists locally?
        S3ConsistentRead s3read = new S3ConsistentRead(this);
        InputStream blockInputStream = s3read.getS3BlockInputStream(b, 0);
        FileUtils.copyInputStreamToFile(blockInputStream, dest);
    }
    
    public void downloadS3BlockTo(String bpid, long blockId, long genStamp, File dest) throws IOException {
        ExtendedBlock b = new ExtendedBlock(bpid, blockId);
        b.setGenerationStamp(genStamp);
        downloadS3BlockTo(b, dest);
    }

    public void downloadS3BlockMetaTo(ExtendedBlock b, File dest) throws IOException {
        // TODO: check cache if block exists locally?
        S3ConsistentRead s3read = new S3ConsistentRead(this);
        InputStream blockMetaInputStream = s3read.getS3BlockMetaInputStream(b);
        FileUtils.copyInputStreamToFile(blockMetaInputStream, dest);
    }

    public void downloadS3BlockMetaTo(String bpid, long blockId, long genStamp, File dest) throws IOException {
        ExtendedBlock b = new ExtendedBlock(bpid, blockId);
        b.setGenerationStamp(genStamp);
        downloadS3BlockMetaTo(b, dest);
    }
    
    @Override // FsDatasetSpi
    public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset) throws IOException {
        // only check for a local block. If not found, assume it's in S3 immediately
        // (since BlockSender checks that before anyway, and if the block doesnt exist we query NN to make sure)
        ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());
        
        if (replicaInfo == null || replicaInfo.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            S3ConsistentRead read = new S3ConsistentRead(this);
            return read.getS3BlockInputStream(b, seekOffset);
        } else {
            return super.getBlockInputStream(b, seekOffset);
        }
    }

    @Override // FsDatasetSpi
    public LengthInputStream getMetaDataInputStream(ExtendedBlock b) throws IOException {
        // only check for a local block. If not found, assume it's in S3 immediately.
        // (since BlockSender checks that before anyway, and if the block doesnt exist we query NN to make sure)
        ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());        

        if (replicaInfo == null || replicaInfo.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            S3ConsistentRead read = new S3ConsistentRead(this);
            return new LengthInputStream(read.getS3BlockMetaInputStream(b), b.getNumBytes());
        } else {
            return super.getMetaDataInputStream(b);
        }
    }


    public static String getBlockKey(ExtendedBlock b) {
        return getBlockKey(b.getBlockPoolId(), b.getBlockId(), b.getGenerationStamp());
    }
    public static String getBlockKey(String blockPoolId, long blockId, long genStamp) {
        return blockPoolId + "/" + BLOCK_FILE_PREFIX + blockId + "_" + genStamp;
    }
    public static String getMetaKey(ExtendedBlock b) {
        return getMetaKey(b.getBlockPoolId(), b.getBlockId(), b.getGenerationStamp());
    }
    public static String getMetaKey(String blockPoolId, long blockId, long genStamp) {
        return getBlockKey(blockPoolId, blockId, genStamp) + METADATA_EXTENSION;
    }
    
    @Override // FsDatasetSpi
    public synchronized ReplicaRecoveryInfo initReplicaRecovery(BlockRecoveryCommand.RecoveringBlock rBlock) throws IOException {
        String bpid = rBlock.getBlock().getBlockPoolId();
        // get the correct replica from S3 that matches given GS
        final ReplicaInfo replica = volumeMap.get(bpid, rBlock.getBlock().getLocalBlock());
        
        if (replica != null && replica.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            // set the finalized dir on the replica
            File finalizedDir = replica.getVolume().getFinalizedDir(bpid);
            File blockDir = DatanodeUtil.idToBlockDir(finalizedDir, replica.getBlockId());
            if (!blockDir.exists()) {
                if (!blockDir.mkdirs()) {
                    throw new IOException("Failed to mkdirs " + blockDir);
                }
            }
            replica.setDir(blockDir);

            
            // Write block to disk
            downloadS3BlockTo(rBlock.getBlock(), replica.getBlockFile());
            downloadS3BlockMetaTo(rBlock.getBlock(), replica.getMetaFile());

            // add downloaded block to volumemap so we can find this exact replica class again
            volumeMap.add(bpid, replica);
        }

        return initReplicaRecovery(bpid, volumeMap,
                rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp(), datanode.getDnConf().getXceiverStopTimeout());
    }


    /**
     * Get the meta info of a block stored in volumeMap. To find a block,
     * block pool Id, block Id and generation stamp must match.
     *
     * @param b
     *     extended block
     * @return the meta replica information; null if block was not found
     * @throws ReplicaNotFoundException
     *     if no entry is in the map or
     *     there is a generation stamp mismatch
     */
    @Override
    public ReplicaInfo getReplicaInfo(ExtendedBlock b) throws ReplicaNotFoundException {
        // volume map only contains non-s3 replicas
        ReplicaInfo info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
        if (info == null) {
            throw new ReplicaNotFoundException(ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
        }
        return info;
    }

    /**
     * Get the meta info of a block stored in volumeMap. Block is looked up
     * without matching the generation stamp.
     *
     * @param bpid
     *     block pool Id
     * @param blkid
     *     block Id
     * @return the meta replica information; null if block was not found
     * @throws ReplicaNotFoundException
     *     if no entry is in the map or
     *     there is a generation stamp mismatch
     */
    // shouldnt have gen stamp check when a block is already downloaded... but before it DL it should check
    @Override
    protected ReplicaInfo getReplicaInfo(String bpid, long blkid) throws ReplicaNotFoundException {
        // volume map only contains non-s3 replicas
        ReplicaInfo info = volumeMap.get(bpid, blkid);
        if (info == null) {
            throw new ReplicaNotFoundException("Replica not found on local file system. Use getReplicaInfo(block) to get from S3 (generation stamp needed). Block" + bpid + ":" + blkid);
        }
        return info;
    }

    @Override // FsDatasetSpi
    public synchronized long getReplicaVisibleLength(final ExtendedBlock block) throws IOException {
        final Replica replica = getReplicaInfo(block);
        if (replica.getGenerationStamp() < block.getGenerationStamp()) {
            throw new IOException(
                    "replica.getGenerationStamp() < block.getGenerationStamp(), block=" +
                            block + ", replica=" + replica);
        }
        return replica.getVisibleLength();
    }

    // Returns S3FinalizedReplica and also checks generation stamps matches
    // Query the namenode for a completed block instead of S3 for consistency --> doesnt work b/c NN has old GS for recovery
    public S3FinalizedReplica getS3FinalizedReplica(ExtendedBlock b) {
        S3ConsistentRead consistentRead = new S3ConsistentRead(this);
        Block block = consistentRead.getS3Block(b);
        if (block != null ) {
            return new S3FinalizedReplica(block, b.getBlockPoolId(), volumes.getVolumes().get(0), bucket);
        }
        return null;
    }


    // This is NOT a read - it doesnt read the block - only gets block information
    // doesnt check GS
//    @Override // FsDatasetSpi
//    public Block getStoredBlock(String bpid, long blkId) {
//        // Look at the volume map for the block; if not found it will query namenode
////        return volumeMap.get(bpid, blkId);
//        // just get the meta
//        return getReplicaInfo(bpid, blkId);
////        S3ConsistentRead consistentRead = new S3ConsistentRead(this);
////        return consistentRead.getS3Block(bpid, blkId);
//    }


    /**
     * Complete the block write!
     */
    @Override // FsDatasetSpi
    public synchronized void finalizeBlock(ExtendedBlock b) throws IOException {
        if (Thread.interrupted()) {
            // Don't allow data modifications from interrupted threads
            throw new IOException("Cannot finalize block from Interrupted Thread");
        }
        ReplicaInfo replicaInfo = getReplicaInfo(b);
        if (replicaInfo.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            // this is legal, when recovery happens on a file that has
            // been opened for append but never modified
            return;
        }
        Date start_time_upload = new Date();
        finalizeReplica(b.getBlockPoolId(), replicaInfo);
        long diffInMillies_upload = (new Date()).getTime() - start_time_upload.getTime();
        LOG.info("=== Upload block " + diffInMillies_upload + " ms ");

        // TODO: performance improvement... defer delete until later? 
        // just delete older block even if it's not there
        Date start_del_time = new Date();
        ExtendedBlock old_b = new ExtendedBlock(b.getBlockPoolId(), b.getBlockId(), b.getNumBytes(), b.getGenerationStamp());
        old_b.setGenerationStamp(old_b.getGenerationStamp() - 1);
        s3afs.delete(new Path(getBlockKey(old_b)), false);
        s3afs.delete(new Path(getMetaKey(old_b)), false);
        long diffInMillies_delete = (new Date()).getTime() - start_del_time.getTime();
        LOG.info("=== Delete prev block time - " + diffInMillies_delete + " ms for append safety.");
    }
    
    /**
     * Complete the block write!
     */
    // TODO: is synchronized still needed for s3?
    private synchronized S3FinalizedReplica finalizeReplica(String bpid, ReplicaInfo replicaInfo) throws IOException {
        File local_block_file = replicaInfo.getBlockFile();
        File local_meta_file = replicaInfo.getMetaFile(); //FsDatasetUtil.getMetaFile(local_block_file, replicaInfo.getGenerationStamp());
        
        // Upload a text string as a new object.
        String s3_block_key = getBlockKey(bpid, replicaInfo.getBlockId(), replicaInfo.getGenerationStamp());
        String s3_block_meta_key = getMetaKey(bpid, replicaInfo.getBlockId(), replicaInfo.getGenerationStamp());


        // Upload a file as a new object with ContentType and title specified.
        PutObjectRequest putReqBlock = new PutObjectRequest(bucket, s3_block_key, local_block_file);
//        ObjectMetadata blockMetadata = new ObjectMetadata();
//        blockMetadata.addUserMetadata("generationstamp", String.valueOf(replicaInfo.getGenerationStamp()));
//        putReqBlock.setMetadata(blockMetadata);
        LOG.info("Uploading block file " + s3_block_key);
        UploadInfo uploadBlock = s3afs.putObject(putReqBlock);
        
        LOG.info("Uploading block meta file " + s3_block_key);
        PutObjectRequest putReqMeta = new PutObjectRequest(bucket, s3_block_meta_key, local_meta_file);
        UploadInfo uploadBlockMeta = s3afs.putObject(putReqMeta);


        // TODO: can this be async??
        // TODO: use S3AFastUpload - https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Stabilizing:_S3A_Fast_Upload
        try {
            uploadBlock.getUpload().waitForUploadResult();
            uploadBlockMeta.getUpload().waitForUploadResult();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error(e);
        }
        
        S3FinalizedReplica newReplicaInfo = new S3FinalizedReplica(
                replicaInfo.getBlockId(), 
                bpid,
                local_block_file.length(), 
                replicaInfo.getGenerationStamp(),
                volumes.getVolumes().get(0),
                bucket);
        
        // remove from volumeMap, so we can get it from s3 instead
        volumeMap.remove(bpid, replicaInfo.getBlockId());
        local_block_file.delete();
        local_meta_file.delete();
        
        return newReplicaInfo;
    }

    /**
     * Append to a finalized replica
     * Change a finalized replica to be a RBW replica and
     * bump its generation stamp to be the newGS
     *
     * @param bpid
     *     block pool Id
     * @param finalizedReplica
     *     a finalized replica
     * @param newGS
     *     new generation stamp
     * @param estimateBlockLen
     *     estimate generation stamp
     * @return a RBW replica
     * @throws IOException
     *     if moving the replica from finalized directory
     *     to rbw directory fails
     */
    @Override
    protected synchronized ReplicaBeingWritten append(String bpid, FinalizedReplica finalizedReplica, long newGS, long estimateBlockLen)
            throws IOException {
        // If the block is cached, start uncaching it.
        cacheManager.uncacheBlock(bpid, finalizedReplica.getBlockId());
        // unlink the finalized replica
        finalizedReplica.unlinkBlock(1);
                
        // TODO: finalizedReplica shouldnt have a vol
        FsVolumeImpl v = (FsVolumeImpl) finalizedReplica.getVolume();
        if (v.getAvailable() < estimateBlockLen - finalizedReplica.getNumBytes()) {
            throw new DiskChecker.DiskOutOfSpaceException(
                    "Insufficient space for appending to " + finalizedReplica);
        }
        
        File newBlkFile = new File(v.getRbwDir(bpid), finalizedReplica.getBlockName());
        
        ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(
                finalizedReplica.getBlockId(), finalizedReplica.getNumBytes(), newGS,
                v, newBlkFile.getParentFile(), Thread.currentThread(), estimateBlockLen);
        File newmeta = newReplicaInfo.getMetaFile();

        // download block file to rbw directory
        LOG.info("Downloading block file to " + newBlkFile + ", file length=" + finalizedReplica.getBytesOnDisk());
        try {
            // Download block file to RBW location
            downloadS3BlockTo(bpid, newReplicaInfo.getBlockId(), finalizedReplica.getGenerationStamp(), newBlkFile);
//            NativeIO.renameTo(oldmeta, newmeta);
        } catch (IOException e) {
            throw new IOException("Block " + finalizedReplica + " reopen failed. " +
                    " Unable to download meta file  to rbw dir " + newmeta, e);
        }

        // rename meta file to rbw directory
        LOG.info("Downloading block meta file to " + newmeta);
        try {
            // Download block meta file to RBW location
            downloadS3BlockMetaTo(bpid, newReplicaInfo.getBlockId(), finalizedReplica.getGenerationStamp(), newmeta);
//            NativeIO.renameTo(blkfile, newBlkFile);
        } catch (IOException e) {
            // delete downloaded meta file 
            newmeta.delete();
            //NativeIO.renameTo(newmeta, oldmeta);

            throw new IOException("Block " + finalizedReplica + " reopen failed. " +
                    " Unable to move download block to rbw dir " + newBlkFile, e);
        }

        // Replace finalized replica by a RBW replica in replicas map
        volumeMap.add(bpid, newReplicaInfo);
        v.reserveSpaceForRbw(estimateBlockLen - finalizedReplica.getNumBytes());
        
        // we dont delete from s3 incase something fails, 
        // the finalizeBlock operation will just replace it anyway. getReplicaInfo() will now return the volumeMap obj
        
        
        return newReplicaInfo;
    }

    /**
     * @deprecated use {@link #fetchReplicaInfo(String, long)} instead.
     */
    @Override
    @Deprecated
    public ReplicaInfo getReplica(String bpid, long blockId) {
        throw new NotImplementedException();
    }

    @Override
    ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
        ReplicaInfo r = volumeMap.get(bpid, blockId);
        if (r == null) {
            throw new CustomRuntimeException("To get an S3 Finalized Replica you need to provide the generation stamp.");
//            return getS3FinalizedReplica(bpid, blockId);
        }
        switch (r.getState()) {
            case FINALIZED:
                LOG.error("Finalized replica must be S3FinalizedReplica - something went wrong here");
                return null;
            case RBW:
                return new ReplicaBeingWritten((ReplicaBeingWritten) r);
            case RWR:
                return new ReplicaWaitingToBeRecovered((ReplicaWaitingToBeRecovered) r);
            case RUR:
                return new ReplicaUnderRecovery((ReplicaUnderRecovery) r);
            case TEMPORARY:
                return new ReplicaInPipeline((ReplicaInPipeline) r);
        }
        return null;
    }


    @Override // FsDatasetSpi
    public synchronized boolean contains(final ExtendedBlock block) {
        final long blockId = block.getLocalBlock().getBlockId();
        try {
            // S3guard marks deleted files, so this would return properly
            // TODO: might need to check GS still
            return s3afs.exists(new Path(getBlockKey(block)));    
        } catch (IOException err) {
            LOG.error(err);
            return false;
        }
    }

    @Override // FsDatasetSpi
    public String toString() {
        return "S3Dataset{bucket='" + bucket + "'}";
    }

    //
    // Methods not needed for s3:
    //
    
    /**
     * Get the list of finalized blocks from in-memory blockmap for a block pool.
     */
    @Override
    public synchronized List<FinalizedReplica> getFinalizedBlocks(String bpid) {
        // TODO: this is for block pool & directory scanners
        //  return empty list for now since s3 is already safe
        return new ArrayList<FinalizedReplica>();
    }
    
    @Override // FsDatasetSpi
    public void cache(String bpid, long[] blockIds) {
        // TODO: preemptively download S3 block and store it in a map?
    }

    @Override // FsDatasetSpi
    public void uncache(String bpid, long[] blockIds) {
        // TODO: preemptively download S3 block and store it in a map?
    }
    

    // we keep these original methods for non-s3 blocks like RBW
//    File getFile(final String bpid, final long blockId) {
//    }
//    File getBlockFile(String bpid, Block b) throws IOException {
//        throw new NotImplementedException();
//    }
    
    /**
     * Find the block's on-disk length
     */
    @Override // FsDatasetSpi
    public long getLength(ExtendedBlock b) throws IOException {
        return getReplicaInfo(b).getNumBytes();
    }
    
    @Override // FsDatasetSpi
    public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block) throws IOException {
        throw new NotImplementedException();
    }
    
     
    // checkAndUpdate() - directory scanner
    @Override
    public void checkAndUpdate(String bpid, long blockId, File diskFile, File diskMetaFile, FsVolumeSpi vol) {
        throw new NotImplementedException();
    }

    public String getBucket() {
        return bucket;
    }

    public DatanodeProtocolClientSideTranslatorPB getNameNodeClient() {
        return namenode;
    }
    
    public S3AFileSystemCommon getS3AFileSystem() {
        return s3afs;
    }
}