/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import io.hops.TestUtil;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TestMetadataLog extends TestCase {
  private static final int ANY_DATASET = -1;
  private static final String ANY_NAME = "-1";

  private static Comparator<MetadataLogEntry> LOGICAL_TIME_COMPARATOR = new Comparator<MetadataLogEntry>() {
    @Override
    public int compare(MetadataLogEntry o1, MetadataLogEntry o2) {
      return Integer.compare(o1.getLogicalTime(), o2.getLogicalTime());
    }
  };

  private boolean checkLog(long inodeId, MetadataLogEntry.Operation operation)
      throws IOException {
    return checkLog(ANY_DATASET, inodeId, operation);
  }

  private boolean checkLog(long datasetId, long inodeId, MetadataLogEntry
      .Operation operation) throws IOException {
    return checkLog(datasetId, inodeId, ANY_NAME, operation);
  }

  private boolean checkLog(long datasetId, long inodeId, String inodeName,
      MetadataLogEntry.Operation operation) throws IOException {
    Collection<MetadataLogEntry> logEntries = getMetadataLogEntries(inodeId);
    for (MetadataLogEntry logEntry : logEntries) {
      if (logEntry.getOperation().equals(operation)) {
        if ((datasetId == ANY_DATASET || datasetId == logEntry.getDatasetId())
            && (inodeName.equals(ANY_NAME) || inodeName.equals(logEntry.getInodeName()))) {
          return true;
        }
      }
    }
    return false;
  }

  private Collection<MetadataLogEntry> getMetadataLogEntries(final long inodeId)
      throws IOException {
    return (Collection<MetadataLogEntry>) new LightWeightRequestHandler(
        HDFSOperationType.GET_METADATA_LOG_ENTRIES) {
      @Override
      public Object performTask() throws IOException {
        MetadataLogDataAccess da = (MetadataLogDataAccess)
            HdfsStorageFactory.getDataAccess(MetadataLogDataAccess.class);
        return da.find(inodeId);
      }
    }.handle();
  }

  @Test
  public void testNonLoggingFolder() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      final Path dataset = new Path(project, "dataset");
      final Path subdir = new Path(dataset, "subdir");
      Path file = new Path(dataset, "file");
      dfs.mkdirs(dataset, FsPermission.getDefault());
      dfs.mkdirs(subdir);
      assertFalse(checkLog(TestUtil.getINodeId(cluster.getNameNode(), subdir),
          MetadataLogEntry.Operation.ADD));
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      assertFalse(checkLog(TestUtil.getINodeId(cluster.getNameNode(), file),
          MetadataLogEntry.Operation.ADD));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCreate() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      final Path dataset = new Path(project, "dataset");
      final Path subdir = new Path(dataset, "subdir");
      Path file = new Path(subdir, "file");
      dfs.mkdirs(dataset, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);
      dfs.mkdirs(subdir);
      assertTrue(checkLog(TestUtil.getINodeId(cluster.getNameNode(), subdir),
          MetadataLogEntry.Operation.ADD));
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      assertTrue(checkLog(TestUtil.getINodeId(cluster.getNameNode(), file),
          MetadataLogEntry.Operation.ADD));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{subdir,
          file}, new int[]{1, 1});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testAppend() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      final Path dataset = new Path(project, "dataset");
      final Path subdir = new Path(dataset, "subdir");
      Path file = new Path(subdir, "file");
      dfs.mkdirs(dataset, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);
      dfs.mkdirs(subdir);
      assertTrue(checkLog(TestUtil.getINodeId(cluster.getNameNode(), subdir),
          MetadataLogEntry.Operation.ADD));
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{subdir,
          file}, new int[]{1, 1});
      
      dfs.append(file).close();
      dfs.append(file).close();

      List<MetadataLogEntry> inodeLogEntries = new
          ArrayList<>(getMetadataLogEntries(inodeId));
      Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);

      assertTrue(inodeLogEntries.size() == 3);
      for(int i=0; i<3;i++){
        assertEquals(i+1, inodeLogEntries.get(i).getLogicalTime());
        assertTrue(inodeLogEntries.get(i).getOperation() ==
            MetadataLogEntry.Operation.ADD);
      }
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{subdir,
          file}, new int[]{1, 3});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testNoLogEntryBeforeClosing() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      final Path dataset = new Path(project, "dataset");
      Path file = new Path(dataset, "file");
      dfs.mkdirs(dataset, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      assertFalse(checkLog(TestUtil.getINodeId(cluster.getNameNode(), file),
          MetadataLogEntry.Operation.ADD));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file},
          new int[]{0});
      out.close();
      assertTrue(checkLog(TestUtil.getINodeId(cluster.getNameNode(), file),
          MetadataLogEntry.Operation.ADD));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file},
          new int[]{1});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDelete() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder = new Path(dataset, "folder");
      Path file = new Path(folder, "file");
      dfs.mkdirs(folder, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      long folderId = TestUtil.getINodeId(cluster.getNameNode(), folder);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder, file},
          new int[]{1,1});
      dfs.delete(folder, true);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.DELETE));

      checkLogicalTimeDeleteAfterAdd(new long[]{folderId, inodeId});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testOldRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset0 = new Path(project, "dataset0");
      Path dataset1 = new Path(project, "dataset1");
      Path file0 = new Path(dataset0, "file");
      Path file1 = new Path(dataset1, "file");
      dfs.mkdirs(dataset0, FsPermission.getDefault());
      dfs.mkdirs(dataset1, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset0, true);
      dfs.setMetaEnabled(dataset1, true);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();
      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file0);
      long dataset1Id = TestUtil.getINodeId(cluster.getNameNode(), dataset1);
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file0},
          new int[]{1});
      assertTrue(dfs.rename(file0, file1));
      assertTrue(checkLog(dataset1Id, inodeId,
          MetadataLogEntry.Operation.RENAME));
      assertEquals(2, getMetadataLogEntries(inodeId).size());
      checkLogicalTimeAddRename(inodeId);
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file1},
          new int[]{2});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void testRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset0 = new Path(project, "dataset0");
      Path dataset1 = new Path(project, "dataset1");
      Path file0 = new Path(dataset0, "file");
      Path file1 = new Path(dataset1, "file");
      dfs.mkdirs(dataset0, FsPermission.getDefault());
      dfs.mkdirs(dataset1, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset0, true);
      dfs.setMetaEnabled(dataset1, true);
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();
      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file0);
      long dataset1Id = TestUtil.getINodeId(cluster.getNameNode(), dataset1);
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file0},
          new int[]{1});
      dfs.rename(file0, file1, Options.Rename.NONE);
      assertTrue(checkLog(dataset1Id, inodeId,
          MetadataLogEntry.Operation.RENAME));
      assertEquals(2, getMetadataLogEntries(inodeId).size());
  
      checkLogicalTimeAddRename(inodeId);
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{file1},
          new int[]{2});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeepOldRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset0 = new Path(project, "dataset0");
      Path folder0 = new Path(dataset0, "folder0");
      Path dataset1 = new Path(project, "dataset1");
      Path folder1 = new Path(dataset1, "folder1");
      Path file0 = new Path(folder0, "file");

      dfs.mkdirs(folder0, FsPermission.getDefault());
      dfs.mkdirs(dataset1, FsPermission.getDefault());

      dfs.setMetaEnabled(dataset0, true);
      dfs.setMetaEnabled(dataset1, true);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();

      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file0);
      long folder0Id = TestUtil.getINodeId(cluster.getNameNode(), folder0);
      long dataset0Id = TestUtil.getINodeId(cluster.getNameNode(), dataset0);
      long dataset1Id = TestUtil.getINodeId(cluster.getNameNode(), dataset1);

      assertTrue(checkLog(dataset0Id, folder0Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(dataset0Id, inodeId, MetadataLogEntry.Operation.ADD));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder0, file0},
          new int[]{1, 1});
      
      dfs.rename(folder0, folder1);

      long folder1Id = TestUtil.getINodeId(cluster.getNameNode(), folder1);
      assertTrue(checkLog(dataset1Id, folder1Id,
          MetadataLogEntry.Operation.RENAME));
      assertTrue(checkLog(dataset1Id, inodeId, MetadataLogEntry.Operation.CHANGEDATASET));
      
      checkLogicalTimeAddRename(folder0Id);
      checkLogicalTimeAddRChangeDataset(inodeId);
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder1,
              new Path(folder1, file0.getName())}, new int[]{2, 2});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeepRename() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset0 = new Path(project, "dataset0");
      Path folder0 = new Path(dataset0, "folder0");
      Path dataset1 = new Path(project, "dataset1");
      Path folder1 = new Path(dataset1, "folder1");
      Path file0 = new Path(folder0, "file");

      dfs.mkdirs(folder0, FsPermission.getDefault());
      dfs.mkdirs(dataset1, FsPermission.getDefault());

      dfs.setMetaEnabled(dataset0, true);
      dfs.setMetaEnabled(dataset1, true);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file0, 1);
      out.close();

      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file0);
      long folder0Id = TestUtil.getINodeId(cluster.getNameNode(), folder0);
      long dataset0Id = TestUtil.getINodeId(cluster.getNameNode(), dataset0);
      long dataset1Id = TestUtil.getINodeId(cluster.getNameNode(), dataset1);

      assertTrue(checkLog(dataset0Id, folder0Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(dataset0Id, inodeId, MetadataLogEntry.Operation.ADD));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder0, file0},
          new int[]{1, 1});
      
      dfs.rename(folder0, folder1, Options.Rename.NONE);

      long folder1Id = TestUtil.getINodeId(cluster.getNameNode(), folder1);
      assertEquals(folder0Id, folder1Id);
      assertTrue(checkLog(dataset1Id, folder0Id,
          MetadataLogEntry.Operation.RENAME));
      assertTrue(checkLog(dataset1Id, inodeId, MetadataLogEntry.Operation.CHANGEDATASET));
  
      checkLogicalTimeAddRename(folder0Id);
      checkLogicalTimeAddRChangeDataset(inodeId);
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder1,
              new Path(folder1, file0.getName())}, new int[]{2, 2});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testEnableLogForExistingDirectory() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder = new Path(dataset, "folder");
      Path file = new Path(folder, "file");
      dfs.mkdirs(folder, FsPermission.getDefault());
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{projects,
          project, dataset, folder, file}, new int[]{0, 0, 0, 0, 0});
      
      dfs.setMetaEnabled(dataset, true);
      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      long folderId = TestUtil.getINodeId(cluster.getNameNode(), folder);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{projects,
          project, dataset, folder, file}, new int[]{0, 0, 0, 1, 1});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeleteFileWhileOpen() throws Exception {
    Configuration conf = new HdfsConfiguration();

    final int BYTES_PER_CHECKSUM = 1;
    final int PACKET_SIZE = BYTES_PER_CHECKSUM;
    final int BLOCK_SIZE = 1 * PACKET_SIZE;
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    try {
      FileSystem fs = cluster.getFileSystem();
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
          .newInstance(fs.getUri(), fs.getConf());

      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder = new Path(dataset, "folder");
      Path file = new Path(folder, "file");
      dfs.mkdirs(folder, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);

      FSDataOutputStream out = dfs.create(file);
      out.writeByte(0);

      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      long folderId = TestUtil.getINodeId(cluster.getNameNode(), folder);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.ADD));
      assertFalse(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder,
          file}, new int[]{1, 0});
      
      DistributedFileSystem dfs2 = (DistributedFileSystem) FileSystem
          .newInstance(fs.getUri(), fs.getConf());

      try {
        dfs2.delete(file, false);
      }catch (Exception ex){
        fail("we shouldn't have any exception: " + ex.getMessage());
      }

      assertFalse(checkLog(inodeId, MetadataLogEntry.Operation.DELETE));
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder},
          new int[]{1});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeepOldRenameInTheSameDataset() throws Exception {
   testDeepRenameInTheSameDataset(true);
  }

  @Test
  public void testDeepRenameInTheSameDataset() throws Exception {
    testDeepRenameInTheSameDataset(false);
  }

  @Test
  public void testOldDeepRenameToNonMetaEnabledDir() throws Exception {
    testDeepRenameToNonMetaEnabledDir(true);
  }

  @Test
  public void testDeepRenameToNonMetaEnabledDir() throws Exception {
    testDeepRenameToNonMetaEnabledDir(false);
  }

  @Test
  public void testDeleteDatasetAfterOldRename() throws Exception {
    testDeleteDatasetAfterRename(true);
  }

  @Test
  public void testDeleteDatasetAfterRename() throws Exception {
    testDeleteDatasetAfterRename(false);
  }

  private void testDeepRenameInTheSameDataset(boolean oldRename) throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder0 = new Path(dataset, "folder0");
      Path folder1 = new Path(folder0, "folder1");
      Path file = new Path(folder1, "file");

      Path newFolder = new Path(dataset, "newFolder");

      dfs.mkdirs(folder1, FsPermission.getDefault());

      dfs.setMetaEnabled(dataset, true);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();

      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      long folder0Id = TestUtil.getINodeId(cluster.getNameNode(), folder0);
      long folder1Id = TestUtil.getINodeId(cluster.getNameNode(), folder1);
      long datasetId = TestUtil.getINodeId(cluster.getNameNode(), dataset);

      assertTrue(checkLog(datasetId, folder0Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(datasetId, folder1Id, MetadataLogEntry.Operation
          .ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder0,
          folder1, file}, new int[]{1, 1, 1});
      
      if(oldRename){
        dfs.rename(folder0, newFolder);
      }else{
        dfs.rename(folder0, newFolder, Options.Rename.NONE);
      }

      long newFolderId = TestUtil.getINodeId(cluster.getNameNode(), newFolder);
      
      assertEquals(folder0Id, newFolderId);
      
      assertTrue(checkLog(datasetId, folder0Id, newFolder.getName(),
          MetadataLogEntry.Operation.RENAME));
      
      assertEquals("Subfolders and files shouldn't be logged during a rename " +
          "in the same dataset", 1, getMetadataLogEntries(folder1Id).size());
      assertEquals("Subfolders and files shouldn't be logged during a rename " +
          "in the same dataset", 1, getMetadataLogEntries(inodeId).size());

      checkLogicalTimeAddRename(folder0Id);
  
      Path newFolder1 = new Path(newFolder, folder1.getName());
      Path newFile = new Path(newFolder1, file.getName());
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{newFolder,
          newFolder1, newFile}, new int[]{2, 1, 1});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void testDeepRenameToNonMetaEnabledDir(boolean oldRename) throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder0 = new Path(dataset, "folder0");
      Path folder1 = new Path(folder0, "folder1");
      Path file = new Path(folder1, "file");

      Path newFolder = new Path(project, "newFolder");

      dfs.mkdirs(folder1, FsPermission.getDefault());

      dfs.setMetaEnabled(dataset, true);

      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();

      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      long folder0Id = TestUtil.getINodeId(cluster.getNameNode(), folder0);
      long folder1Id = TestUtil.getINodeId(cluster.getNameNode(), folder1);
      long datasetId = TestUtil.getINodeId(cluster.getNameNode(), dataset);

      assertTrue(checkLog(datasetId, folder0Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(datasetId, folder1Id, MetadataLogEntry.Operation
          .ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder0,
          folder1, file}, new int[]{1, 1, 1});
      
      if(oldRename){
        dfs.rename(folder0, newFolder);
      }else{
        dfs.rename(folder0, newFolder, Options.Rename.NONE);
      }

      long newFolderId = TestUtil.getINodeId(cluster.getNameNode(), newFolder);
      assertTrue(checkLog(datasetId, folder0Id,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(datasetId, folder1Id,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(datasetId, inodeId,
          MetadataLogEntry.Operation.DELETE));
      assertFalse(checkLog(datasetId, newFolderId, newFolder.getName(),
          MetadataLogEntry.Operation.ADD));
      assertEquals("Subfolders and files shouldn't be logged for addition " +
          "during a move to a non MetaEnabled directoy", 2,
          getMetadataLogEntries(folder1Id).size());
      assertEquals("Subfolders and files shouldn't be logged for addition " +
              "during a move to a non MetaEnabled directoy", 2,
          getMetadataLogEntries(inodeId).size());

      //Check logical times
      checkLogicalTimeDeleteAfterAdd(new long[]{folder0Id, folder1Id, inodeId});
  
      Path newFolder1 = new Path(newFolder, folder1.getName());
      Path newFile = new Path(newFolder1, file.getName());
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{newFolder,
          newFolder1, newFile}, new int[]{2, 2, 2});
      
      //Move the directory back to the dataset
      if(oldRename){
        dfs.rename(newFolder, folder0);
      }else{
        dfs.rename(newFolder, folder0, Options.Rename.NONE);
      }
  
      assertTrue(checkLog(datasetId, folder0Id, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(datasetId, folder1Id, MetadataLogEntry.Operation
          .ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      
      checkLogicalTimeAddDeleteAdd(folder0Id);
      checkLogicalTimeAddDeleteAdd(folder1Id);
      checkLogicalTimeAddDeleteAdd(inodeId);
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder0,
          folder1, file}, new int[]{3, 3, 3});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void testDeleteDatasetAfterRename(boolean oldRename) throws
      IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path dataset = new Path("/dataset");
      Path folder1 = new Path(dataset, "folder1");
      Path folder2 = new Path(folder1, "folder2");
      Path folder3 = new Path(folder2, "folder3");

      dfs.mkdirs(folder3, FsPermission.getDefault());
      dfs.setMetaEnabled(dataset, true);

      long datasetId = TestUtil.getINodeId(cluster.getNameNode(), dataset);
      long folder1Id = TestUtil.getINodeId(cluster.getNameNode(), folder1);
      long folder2Id = TestUtil.getINodeId(cluster.getNameNode(), folder2);
      long folder3Id = TestUtil.getINodeId(cluster.getNameNode(), folder3);

      assertTrue(checkLog(datasetId, folder1Id,
          MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(datasetId, folder2Id,
          MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(datasetId, folder3Id,
          MetadataLogEntry.Operation.ADD));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder1,
          folder2, folder3}, new int[]{1, 1, 1});
      
      Path file1 = new Path(folder3, "file1");
      TestFileCreation.create(dfs, file1, 1).close();

      Path file2 = new Path(folder3, "file2");
      TestFileCreation.create(dfs, file2, 1).close();

      long file1Id = TestUtil.getINodeId(cluster.getNameNode(), file1);
      long file2Id = TestUtil.getINodeId(cluster.getNameNode(), file2);

      assertTrue(checkLog(datasetId, file1Id,
          MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(datasetId, file2Id,
          MetadataLogEntry.Operation.ADD));
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder1,
          folder2, folder3, file1, file1}, new int[]{1, 1, 1, 1, 1});
      
      Path newDataset = new Path("/newDataset");

      if(oldRename){
        dfs.rename(dataset, newDataset);
      }else{
        dfs.rename(dataset, newDataset, Options.Rename.NONE);
      }

      long newDatasetId = TestUtil.getINodeId(cluster.getNameNode(), newDataset);
      assertTrue(newDatasetId == datasetId);

      assertFalse(checkLog(datasetId, folder1Id,
          MetadataLogEntry.Operation.DELETE));
      assertFalse(checkLog(datasetId, folder2Id,
          MetadataLogEntry.Operation.DELETE));
      assertFalse(checkLog(datasetId, folder3Id,
          MetadataLogEntry.Operation.DELETE));
      assertFalse(checkLog(datasetId, file1Id,
          MetadataLogEntry.Operation.DELETE));
      assertFalse(checkLog(datasetId, file2Id,
          MetadataLogEntry.Operation.DELETE));
  
      Path newFolder1 = new Path(newDataset, folder1.getName());
      Path newFolder2 = new Path(newFolder1, folder2.getName());
      Path newFolder3 = new Path(newFolder2, folder3.getName());
      Path newFile1 = new Path(newFolder3, file1.getName());
      Path newFile2 = new Path(newFolder3, file2.getName());
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{newFolder1,
          newFolder2, newFolder3, newFile1, newFile2}, new int[]{1, 1, 1, 1, 1});
      
      assertTrue(dfs.delete(newDataset, true));

      assertTrue(checkLog(datasetId, folder1Id,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(datasetId, folder2Id,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(datasetId, folder3Id,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(datasetId, file1Id,
          MetadataLogEntry.Operation.DELETE));
      assertTrue(checkLog(datasetId, file2Id,
          MetadataLogEntry.Operation.DELETE));

      checkLogicalTimeDeleteAfterAdd(new long[]{folder1Id, folder2Id, folder3Id, file1Id,
          file2Id});

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  
  @Test
  public void testSettingAndUnsettingMetaEnabled() throws Exception{
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .build();
    try {
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path projects = new Path("/projects");
      Path project = new Path(projects, "project");
      Path dataset = new Path(project, "dataset");
      Path folder = new Path(dataset, "folder");
      Path file = new Path(folder, "file");
      dfs.mkdirs(folder, FsPermission.getDefault());
      HdfsDataOutputStream out = TestFileCreation.create(dfs, file, 1);
      out.close();
      dfs.setMetaEnabled(dataset, true);
      long inodeId = TestUtil.getINodeId(cluster.getNameNode(), file);
      long folderId = TestUtil.getINodeId(cluster.getNameNode(), folder);
      assertTrue(checkLog(folderId, MetadataLogEntry.Operation.ADD));
      assertTrue(checkLog(inodeId, MetadataLogEntry.Operation.ADD));
      
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder,
          file}, new int[]{1, 1});

      dfs.setMetaEnabled(dataset, false);
      
      assertEquals(1, getMetadataLogEntries(inodeId).size());
      assertEquals(1, getMetadataLogEntries(folderId).size());
  
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder,
          file}, new int[]{1, 1});
      dfs.setMetaEnabled(dataset, true);
  
      assertEquals(2, getMetadataLogEntries(inodeId).size());
      assertEquals(2, getMetadataLogEntries(folderId).size());
      checkLogicalTimeForINodes(cluster.getNameNode(), new Path[]{folder,
          file}, new int[]{2, 2});
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private void checkLogicalTimeDeleteAfterAdd(long[] inodesIds) throws
      IOException {
    for(long inodeId : inodesIds){
      List<MetadataLogEntry> inodeLogEntries = new
          ArrayList<>(getMetadataLogEntries(inodeId));
      Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
      assertTrue(inodeLogEntries.size() == 2);
      assertTrue(inodeLogEntries.get(0).getOperation() ==
          MetadataLogEntry.Operation.ADD);
      assertTrue(inodeLogEntries.get(1).getOperation() ==
          MetadataLogEntry.Operation.DELETE);
    }
  }
  
  private void checkLogicalTimeAddDeleteAdd(long inodeId) throws IOException {
    List<MetadataLogEntry> inodeLogEntries = new
        ArrayList<>(getMetadataLogEntries(inodeId));
    
    Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(inodeLogEntries.size() == 3);
    assertTrue(inodeLogEntries.get(0).getOperation() ==
        MetadataLogEntry.Operation.ADD);
    assertTrue(inodeLogEntries.get(1).getOperation() ==
        MetadataLogEntry.Operation.DELETE);
    assertTrue(inodeLogEntries.get(2).getOperation() ==
        MetadataLogEntry.Operation.ADD);
  }
  
  private void checkLogicalTimeAddRename(long inodeId) throws IOException {
    List<MetadataLogEntry> inodeLogEntries = new
        ArrayList<>(getMetadataLogEntries(inodeId));
    
    Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(inodeLogEntries.size() == 2);
    assertTrue(inodeLogEntries.get(0).getOperation() ==
        MetadataLogEntry.Operation.ADD);
    assertTrue(inodeLogEntries.get(1).getOperation() ==
        MetadataLogEntry.Operation.RENAME);
  }
  
  private void checkLogicalTimeAddRChangeDataset(long inodeId) throws
      IOException {
    List<MetadataLogEntry> inodeLogEntries = new
        ArrayList<>(getMetadataLogEntries(inodeId));
    
    Collections.sort(inodeLogEntries, LOGICAL_TIME_COMPARATOR);
    assertTrue(inodeLogEntries.size() == 2);
    assertTrue(inodeLogEntries.get(0).getOperation() ==
        MetadataLogEntry.Operation.ADD);
    assertTrue(inodeLogEntries.get(1).getOperation() ==
        MetadataLogEntry.Operation.CHANGEDATASET);
  }
  
  private void checkLogicalTimeForINodes(NameNode nameNode, Path[] inodesPaths,
      int[] logicalTimes) throws IOException{
    int i=0;
    for(Path path : inodesPaths){
      assertEquals(logicalTimes[i], TestUtil.getINode(nameNode, path)
          .getLogicalTime());
      i++;
    }
  }
  
}
