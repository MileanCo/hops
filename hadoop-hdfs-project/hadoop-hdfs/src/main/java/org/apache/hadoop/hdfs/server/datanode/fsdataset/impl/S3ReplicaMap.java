package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;

public class S3ReplicaMap extends ReplicaMap {
    
    private S3DatasetImpl s3Dataset;

    S3ReplicaMap(Object mutex, S3DatasetImpl s3Dataset) {
        super(mutex);
        this.s3Dataset = s3Dataset;
    }

    /**
     * Get the meta information of the replica that matches both block id
     * and generation stamp. If replicaMap does not contain the block, we check S3.
     *
     * @param bpid
     *     block pool id
     * @param block
     *     block with its id as the key
     * @return the replica's meta information
     * @throws IllegalArgumentException
     *     if the input block or block pool is null
     */
    @Override
    ReplicaInfo get(String bpid, Block block) {
        // first get from volume map like normally
        ReplicaInfo replicaInfo = super.get(bpid, block.getBlockId());
        // check S3 if replicainfo is null
        if (replicaInfo == null) {
            // Get block from S3 consistently
            replicaInfo = s3Dataset.getS3FinalizedReplica(block.getGenerationStamp(), bpid, block.getBlockId());
        }
        // check gen stamp and return
        if (replicaInfo != null && block.getGenerationStamp() == replicaInfo.getGenerationStamp()) {
            return replicaInfo;
        }
        return replicaInfo;
    }
    

    // Dont override this function, since RBW blocks are local.
    //    ReplicaInfo get(String bpid, long blockId) {
}
