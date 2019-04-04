package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.util.DiskChecker;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocol.Block.BLOCK_FILE_PREFIX;
import static org.apache.hadoop.hdfs.protocol.Block.METADATA_EXTENSION;

//class S3DatasetImpl implements FsDatasetSpi<FsVolumeImpl> {

public class S3DatasetImpl extends FsDatasetImpl {
    AmazonS3 s3Client;
    String bucket;
    
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
        bucket = conf.get("S3_BUCKET_URI");
        
        String region = conf.get("fs.s3a.endpoint");
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
    }
    
    public void downloadS3BlockTo(String bpid, long blockId, File dest) throws IOException {
        // TODO: check cache if block exists locally?
        LengthInputStream blockInputStream = getS3BlockInputStream(bpid, blockId, 0);
        FileUtils.copyInputStreamToFile(blockInputStream, dest);
    }

    public void downloadS3BlockMetaTo(String bpid, long blockId, File dest) throws IOException {
        // TODO: check cache if block exists locally?
        LengthInputStream blockMetaInputStream = getS3BlockMetaInputStream(bpid, blockId);
        FileUtils.copyInputStreamToFile(blockMetaInputStream, dest);
    }
    
    public LengthInputStream getS3BlockInputStream(String bpid, long blockId, long seekOffset) {
        String block_aws_key = getBlockKey(bpid, blockId);
        LOG.info("Getting block file " + block_aws_key);
        
        try {
            GetObjectRequest getRequest = new GetObjectRequest(bucket, block_aws_key);
            getRequest.setRange(seekOffset);
            S3Object fullObject = s3Client.getObject(getRequest);
            return new LengthInputStream(fullObject.getObjectContent(), fullObject.getObjectMetadata().getInstanceLength());
        } catch (AmazonS3Exception err) {
            LOG.error(err);
//            datanode.query_nn(blkid);
            return null;
        }
    }

    public LengthInputStream getS3BlockMetaInputStream(String bpid, long blockId) {
        String block_meta_aws_key = getBlockMetaKey(bpid, blockId);
        LOG.info("Getting block meta file " + block_meta_aws_key);
        try {
            S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucket, block_meta_aws_key));
            return new LengthInputStream(fullObject.getObjectContent(), fullObject.getObjectMetadata().getInstanceLength());
        } catch (AmazonS3Exception err) {
            LOG.error(err);
//            datanode.query_nn(blkid);
            return null;
        }
    }
    
    @Override // FsDatasetSpi
    public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset) throws IOException {
        // TODO: can probably prevent an S3 query here by changing input params or checking them
        ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock().getBlockId());
        
        if (replicaInfo == null || replicaInfo.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            return getS3BlockInputStream(b.getBlockPoolId(), b.getLocalBlock().getBlockId(), seekOffset);
        } else {
            return super.getBlockInputStream(b, seekOffset);
        }
    }

    @Override // FsDatasetSpi
    public LengthInputStream getMetaDataInputStream(ExtendedBlock b) throws IOException {
        // TODO: can probably prevent an S3 query here by changing input params or checking them
        ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());

        if (replicaInfo == null || replicaInfo.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            return getS3BlockMetaInputStream(b.getBlockPoolId(), b.getBlockId());
        } else {
            return super.getMetaDataInputStream(b);
        }
    }
    
    
    public static String getBlockKey(String blockPoolId, long blockId) {
        return blockPoolId + "/" + BLOCK_FILE_PREFIX + blockId;
    }
    public static String getBlockMetaKey(String blockPoolId, long blockId) {
        return getBlockKey(blockPoolId, blockId) + METADATA_EXTENSION;
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
    ReplicaInfo getReplicaInfo(ExtendedBlock b) throws ReplicaNotFoundException {
        // volume map only contains non-s3 replicas
        ReplicaInfo info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
        if (info == null) {
            info = getS3FinalizedReplica(b.getBlockPoolId(), b.getLocalBlock());
            if (info == null) {
                throw new ReplicaNotFoundException(ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
            }
        }
        return info;
    }
    
    

    
    @Override // FsDatasetSpi
    public synchronized ReplicaRecoveryInfo initReplicaRecovery(BlockRecoveryCommand.RecoveringBlock rBlock) throws IOException {
        String bpid = rBlock.getBlock().getBlockPoolId();
        final ReplicaInfo replica = volumeMap.get(bpid, rBlock.getBlock().getBlockId());
        
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
            downloadS3BlockTo(bpid, replica.getBlockId(), replica.getBlockFile());
            downloadS3BlockMetaTo(bpid, replica.getBlockId(), replica.getMetaFile());

            // add downloaded block to volumemap so we can find this exact replica class again
            // TODO: delete local block after recovery is complete??
            volumeMap.add(bpid, replica);
        }

        return initReplicaRecovery(bpid, volumeMap,
                rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp(), datanode.getDnConf().getXceiverStopTimeout());
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
    @Override
    protected ReplicaInfo getReplicaInfo(String bpid, long blkid) throws ReplicaNotFoundException {
        // volume map only contains non-s3 replicas
        ReplicaInfo info = volumeMap.get(bpid, blkid);
        if (info == null) {
            info = getS3FinalizedReplica(bpid, blkid);
            if (info == null) {
                throw new ReplicaNotFoundException(ReplicaNotFoundException.NON_EXISTENT_REPLICA + bpid + ":" + blkid);
            }
        }
        return info;
    }

    // Returns S3FinalizedReplica and also checks generation stamps matches
    public S3FinalizedReplica getS3FinalizedReplica(String bpid, Block block) {
        S3FinalizedReplica s3replica = getS3FinalizedReplica(bpid, block.getBlockId());
        if (s3replica != null && block.getGenerationStamp() == s3replica.getGenerationStamp()) {
            return s3replica;
        }
        return null;
    }


    public S3FinalizedReplica getS3FinalizedReplica(String bpid, long blockId) {
        Block block = getStoredBlock(bpid, blockId);
        if (block != null) {
            return new S3FinalizedReplica(block, bpid, volumes.getVolumes().get(0), bucket);    
        }
        return null;
    }

    // This is NOT a read - it doesnt read the block - only gets block information
    @Override // FsDatasetSpi
    public Block getStoredBlock(String bpid, long blkId) {
//        File blockfile = getFile(bpid, blkid);
//        if (blockfile == null) {
//            return null;
//        }
        String block_aws_key = getBlockKey(bpid, blkId);
        S3Object blockObj;
        try {
            blockObj = s3Client.getObject(new GetObjectRequest(bucket, block_aws_key));
        } catch (AmazonS3Exception err) {
            LOG.error(err);
//            datanode.query_nn(blkid);
            return null;
        }
//        final File metafile = FsDatasetUtil.findMetaFile(blockfile);
//        final long gs = FsDatasetUtil.parseGenerationStamp(blockfile, metafile);
        long gs = Long.parseLong(blockObj.getObjectMetadata().getUserMetadata().get("generationstamp"));

        return new Block(blkId, blockObj.getObjectMetadata().getInstanceLength(), gs);
    }
    
    /**
     * Complete the block write!
     */
    // TODO: is synchronized still needed for s3?
    @Override
    protected synchronized S3FinalizedReplica finalizeReplica(String bpid, ReplicaInfo replicaInfo) throws IOException {
//        FinalizedReplica newReplicaInfo;
//            FsVolumeImpl v = (FsVolumeImpl) replicaInfo.getVolume();
//            File f = replicaInfo.getBlockFile();
//            if (v == null) {
//                throw new IOException("No volume for temporary file " + f +
//                        " for block " + replicaInfo);
//            }
//    
//            File dest = v.addFinalizedBlock(bpid, replicaInfo, f, replicaInfo.getBytesReserved());
//            
        File local_block_file = replicaInfo.getBlockFile();
        File local_meta_file = replicaInfo.getMetaFile(); //FsDatasetUtil.getMetaFile(local_block_file, replicaInfo.getGenerationStamp());


        // Upload a text string as a new object.
        String s3_block_key = getBlockKey(bpid, replicaInfo.getBlockId());
        String s3_block_meta_key = getBlockMetaKey(bpid, replicaInfo.getBlockId());


        // Upload a file as a new object with ContentType and title specified.
        PutObjectRequest putReqBlock = new PutObjectRequest(bucket, s3_block_key, local_block_file);
        ObjectMetadata blockMetadata = new ObjectMetadata();
//            metadata.setContentType("plain/text");
        blockMetadata.addUserMetadata("generationstamp", String.valueOf(replicaInfo.getGenerationStamp()));
        putReqBlock.setMetadata(blockMetadata);
        LOG.info("Putting block file " + s3_block_key);
        s3Client.putObject(putReqBlock);
        
        LOG.info("Putting block meta file " + s3_block_key);
        PutObjectRequest putReqMeta = new PutObjectRequest(bucket, s3_block_meta_key, local_meta_file);
        s3Client.putObject(putReqMeta);
        

        // TODO localBlockFile.getTotalSpace() ?
        //  GS++ ?
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
        
        // construct a RBW replica with the new GS
//        File blkfile = replicaInfo.getBlockFile();
        
        // TODO: finalizedReplica shouldnt have a vol
        FsVolumeImpl v = (FsVolumeImpl) finalizedReplica.getVolume();
        if (v.getAvailable() < estimateBlockLen - finalizedReplica.getNumBytes()) {
            throw new DiskChecker.DiskOutOfSpaceException(
                    "Insufficient space for appending to " + finalizedReplica);
        }
        
        File newBlkFile = new File(v.getRbwDir(bpid), finalizedReplica.getBlockName());
//        String old_meta_path = getBlockMetaKey(bpid, finalizedReplica.getBlockId());
//        File oldmeta = finalizedReplica.getMetaFile();
        
        ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(
                finalizedReplica.getBlockId(), finalizedReplica.getNumBytes(), newGS,
                v, newBlkFile.getParentFile(), Thread.currentThread(), estimateBlockLen);
        File newmeta = newReplicaInfo.getMetaFile();

        // rename meta file to rbw directory
        if (LOG.isDebugEnabled()) {
            LOG.debug("Downloading block meta file to " + newmeta);
        }
        
        try {
            // Download block file to RBW location
            downloadS3BlockTo(bpid, newReplicaInfo.getBlockId(), newBlkFile);
//            NativeIO.renameTo(oldmeta, newmeta);
        } catch (IOException e) {
            throw new IOException("Block " + finalizedReplica + " reopen failed. " +
                    " Unable to download meta file  to rbw dir " + newmeta, e);
        }

        // download block file to rbw directory
        if (LOG.isDebugEnabled()) {
            LOG.debug("Downloading block file to " + newBlkFile + ", file length=" + finalizedReplica.getBytesOnDisk());
        }
        try {
            // Download block meta file to RBW location
            downloadS3BlockMetaTo(bpid, newReplicaInfo.getBlockId(), newmeta);
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
        ReplicaInfo rep = volumeMap.get(bpid, blockId);
        if (rep == null) {
            return getS3FinalizedReplica(bpid, blockId);
        }
        return rep;
    }

    @Override
    ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
        ReplicaInfo r = volumeMap.get(bpid, blockId);
        if (r == null) {
            return getS3FinalizedReplica(bpid, blockId);
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
        return s3Client.doesObjectExist(bucket, getBlockKey(block.getBlockPoolId(), blockId));
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
        // this is fory block pool & directory scanners
        throw new NotImplementedException();    
    }
    
    @Override // FsDatasetSpi
    public void cache(String bpid, long[] blockIds) {
        // TODO: preemptively download S3 block and store it in a map?
    }

    @Override // FsDatasetSpi
    public void uncache(String bpid, long[] blockIds) {
        // TODO: preemptively download S3 block and store it in a map?
    }
    /**
     * Turn the block identifier into a filename
     *
     * @param bpid
     *     Block pool Id
     * @param blockId
     *     a block's id
     * @return on disk data file path; null if the replica does not exist
     */
    @Override
    File getFile(final String bpid, final long blockId) {
        throw new NotImplementedException();
//        throw new Exception("S3 doesnt use local file paths");
//        ReplicaInfo info = volumeMap.get(bpid, blockId);
//        S3FinalizedReplica s3blockInfo = downloadedBlocksMap.get(bpid).get(blockId);
//        if (s3blockInfo != null) {
////            return info.getBlockFile();
//            return s3blockInfo.get
//        }
//        return null;
    }
    /**
     * Find the block's on-disk length
     */
    @Override // FsDatasetSpi
    public long getLength(ExtendedBlock b) throws IOException {
        // query S3 and return that? Not used for S3 really.
        // TODO: performance improv
        return getStoredBlock(b.getBlockPoolId(), b.getBlockId()).getNumBytes();
//        return getBlockFile(b).length();
    }

    /**
     * Get File name for a given block.
     */
    @Override
    File getBlockFile(String bpid, Block b) throws IOException {
        throw new NotImplementedException();
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
    
}
