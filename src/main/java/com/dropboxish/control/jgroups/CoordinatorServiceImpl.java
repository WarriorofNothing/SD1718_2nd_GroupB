package com.dropboxish.control.jgroups;

import com.dropboxish.control.fec.FEC;
import com.dropboxish.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.SerializationUtils;
import org.jgroups.JChannel;
import org.jgroups.Message;

import java.io.*;
import java.sql.*;
import java.util.*;

public class CoordinatorServiceImpl extends ApplicationServiceGrpc.ApplicationServiceImplBase{
    private JChannel jChannel;
    private List<StoragePool> poolsList;
    private Connection connectionDB;

    CoordinatorServiceImpl(JChannel channel, List<StoragePool> poolsList, Connection connectionDB){
        this.jChannel = channel;
        this.poolsList = poolsList;
        this.connectionDB = connectionDB;
    }

    @Override
    public void relayFile(RelayRequest request, StreamObserver<RelayResponse> responseObserver) {
        String fileName = request.getFileName();
        String fileHash = request.getFileHash();

        /*if(checkFile(fileName)) {
            responseObserver.onCompleted();
            return;
        }*/

        ManagedChannel grpcChannel = ManagedChannelBuilder.forAddress("127.0.0.1", 8081)
                .usePlaintext(true)
                .build();

        ApplicationServiceGrpc.ApplicationServiceBlockingStub stub  = ApplicationServiceGrpc.newBlockingStub(grpcChannel);

        Iterator<Chunk> chunkData = stub.getFile(FileRequest.newBuilder()
                .setFileName(fileName)
                .setFileHash(fileHash)
                .build());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while(chunkData.hasNext()) {
            byte[] chunk_received = chunkData.next().getChunk().toByteArray();
            baos.write(chunk_received, 0, chunk_received.length);
        }

        byte[] block = baos.toByteArray();

        grpcChannel.shutdown();

        if(poolsList.size() >= FEC.getTotalShards()) {
            List<byte[]> shards = FEC.encode(block);

            boolean anyShardMissing = false;

            int i = 0;
            for (byte[] shard : shards) {
                Shard shardIndexed = new Shard(shard,i);

                if(!poolsList.get(i).putShard(fileName,shardIndexed,false)){
                    anyShardMissing = true;
                }

                CoordinatorMessage controlMessage = new CoordinatorMessage(2, fileName, shardIndexed, i);
                byte[] buffer = SerializationUtils.serialize(controlMessage);

                Message msg = new Message(null, buffer);

                try {
                    jChannel.send(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                i++;
            }

            if(!anyShardMissing){
                //insertIntoDB(fileName,fileHash,block.length);
                System.out.println("** [SERVICE-INFO] File <" + fileName + "> has been encoded and stored successfully");

                responseObserver.onNext(RelayResponse.newBuilder()
                        .setStatus("OK")
                        .build());
            } else {
                responseObserver.onNext(RelayResponse.newBuilder()
                        .setStatus("ERROR")
                        .build());
            }

        } else{
            System.out.println("** [SERVICE-INFO] Not enough storage pools to store the file");
            responseObserver.onNext(RelayResponse.newBuilder()
                    .setStatus("ERROR")
                    .build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void getFile(FileRequest request, StreamObserver<Chunk> responseObserver) {
        String fileName = request.getFileName();

        HashMap<Integer,byte[]> shards = new HashMap<>();

        for(StoragePool pool : poolsList){
            Shard shard;
            if((shard = pool.getShard(fileName)) != null)
                shards.put(shard.getIndex(), shard.getData());

        }

        if(shards.size() > 0) {
            byte[] block = FEC.decode(shards);

            if (block != null) {
                int chunkMaxSize = 4096;
                int bytesRead = 0;
                while (bytesRead < block.length) {
                    byte[] chunk = Arrays.copyOfRange(block, bytesRead, bytesRead + chunkMaxSize);

                    responseObserver.onNext(Chunk.newBuilder()
                            .setChunk(ByteString.copyFrom(chunk))
                            .build());

                    bytesRead = bytesRead + chunk.length;
                }
            } else System.out.println("** [SERVICE-INFO] Couldn't decode the file <" + fileName + ">");
        }

        responseObserver.onCompleted();
    }

    @Override
    public void relayDelete(RelayRequest request, StreamObserver<RelayResponse> responseObserver) {
        String fileName = request.getFileName();

        /*if(!checkFile(fileName)){
            responseObserver.onCompleted();
            return;
        }*/

        boolean anyShardLeft = false;

        for(StoragePool pool : poolsList)
            if(!pool.deleteShard(fileName))
                anyShardLeft = true;

        CoordinatorMessage controlMessage = new CoordinatorMessage(3, fileName);
        byte[] buffer = SerializationUtils.serialize(controlMessage);

        Message msg = new Message(null, buffer);

        try {
            jChannel.send(msg);
        } catch (Exception e) {
            System.out.println("[ERROR] Message was not sent to the group");
        }

        if(!anyShardLeft){
            //deleteFromDB(fileName);
            System.out.println("** [SERVICE-INFO] File <" + fileName + "> has been deleted successfully");

            responseObserver.onNext(RelayResponse.newBuilder()
                    .setStatus("OK")
                    .build());
        } else {
            System.out.println("** [SERVICE-INFO] File <" + fileName + "> has not been deleted successfully");
            responseObserver.onNext(RelayResponse.newBuilder()
                    .setStatus("ERROR")
                    .build());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void getList(RelayRequest request, StreamObserver<ListResponse> responseObserver) {
        if(connectionDB == null){
            responseObserver.onNext(ListResponse.newBuilder()
                    .setFileName("ERROR")
                    .build());

            responseObserver.onCompleted();

            return;
        }


        try{
            String pattern = request.getPattern().toLowerCase();
            String query = "SELECT * FROM files";

            Statement statement = connectionDB.createStatement();
            ResultSet rs = statement.executeQuery(query);

            while (rs.next()) {

                String fileName = rs.getString("name");
                long fileSize = rs.getLong("size");

                if(fileName.toLowerCase().contains(pattern)) {
                    responseObserver.onNext(ListResponse.newBuilder()
                            .setFileName(fileName)
                            .setFileSize(fileSize)
                            .build());
                }
            }

        } catch (SQLException e){
            System.out.println("[ERROR] Couldn't get files info from Database");
        }

        responseObserver.onCompleted();
    }

    private boolean checkFile(String fileName){
        try {
            String query = "SELECT * FROM files WHERE name = '" + fileName + "'";

            Statement statement = connectionDB.createStatement();
            ResultSet rs = statement.executeQuery(query);

            return rs.next();

        } catch (SQLException e){
            return false;
        }
    }

    private void insertIntoDB(String fileName, String fileHash, long fileSize){
        try{
            String query = "INSERT INTO files (name, hash, size) VALUES (?, ?, ?)";

            PreparedStatement preparedStmt = connectionDB.prepareStatement(query);
            preparedStmt.setString (1, fileName);
            preparedStmt.setString (2, fileHash);
            preparedStmt.setLong    (3, fileSize);

            preparedStmt.execute();

        } catch (SQLException e){
            System.out.println("[ERROR] Couldn't insert into Database");
        }
    }

    private void deleteFromDB(String fileName){
        try{
            String query = "DELETE FROM files WHERE name = ?";
            PreparedStatement preparedStmt = connectionDB.prepareStatement(query);
            preparedStmt.setString(1, fileName);

            preparedStmt.execute();

        } catch (SQLException e){
            System.out.println("[ERROR] Couldn't delete file from Database");
        }
    }
}
