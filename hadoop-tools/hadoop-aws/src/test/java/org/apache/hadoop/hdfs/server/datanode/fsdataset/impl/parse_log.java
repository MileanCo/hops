//package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class parse_log {
    public static void parse_log_file(long total_time, String out) {
        // read captured stdout and count data
        int dfs_create_time = 0;
        int createRBW_time = 0;
        int receiveBlock_time = 0;
        int new_BlockReceiver_time = 0;
        int packet_responder_time = 0;
        int finalizeBlk_time=0;
        int upload_time = 0;
        int delete_time = 0;
        int opWriteBlock = 0;
        int completeFile_time = 0;

        String[] lines = out.split("\n");
        for (int i=0; i<lines.length;i++) {
            String[] parts = lines[i].split(" ");
            if (lines[i].contains("createRBW time")) {
                createRBW_time += Integer.parseInt(parts[2]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("DFS.create")) {
                dfs_create_time += Integer.parseInt(parts[1]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("new_BlockReceiver")) {
                new_BlockReceiver_time += Integer.parseInt(parts[1]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("receiveBlock_time")) {
                receiveBlock_time += Integer.parseInt(parts[1]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("packet_responder")) {
                packet_responder_time += Integer.parseInt(parts[1]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("finalizeBlk_time")) {
                finalizeBlk_time += Integer.parseInt(parts[1]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("Upload")) {
                upload_time += Integer.parseInt(parts[3]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("Delete")) {
                delete_time += Integer.parseInt(parts[6]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("opWriteblock")) {
                opWriteBlock += Integer.parseInt(parts[1]);
                System.out.println(lines[i]);
            } else if (lines[i].contains("completeFile")) {
                completeFile_time += Integer.parseInt(parts[1]);
                System.out.println(lines[i]);
            }
        }

        System.out.println("------------\n");
        System.out.println("dfs_create_time: " + dfs_create_time);
        System.out.println("new_BlockReceiver time: " + new_BlockReceiver_time + " (createRBW time: " + createRBW_time + ")" );
        System.out.println("receiveBlock_time : " + receiveBlock_time);
        System.out.println("packet_responder_time: " + packet_responder_time);
        System.out.println("finalizeBlk_time: " + finalizeBlk_time);
        System.out.println("Upload time: " + upload_time);
        System.out.println("S3Finalized.delete time: " + delete_time);
        System.out.println("opWriteBlock time: " + opWriteBlock);
        System.out.println("completeFile_time time: " + completeFile_time);

        double diffInSec = total_time / 1000.0;
        System.out.println("-----------------------\n" +
                "It took " + diffInSec + " seconds to write" +
                "\n---------------------------\n\n\n");
    }
    
    public static void main(String[] args) {
        try {
            String file_path = args[0];

            String log_file_str = new String(Files.readAllBytes(Paths.get(file_path)), StandardCharsets.UTF_8);

            parse_log_file(0, log_file_str);
            System.exit(0);
        } catch (IOException e) {
//            System.out.println("#### Exception in Main");
            e.printStackTrace();
            System.exit(-2);
        }
    }
}

    