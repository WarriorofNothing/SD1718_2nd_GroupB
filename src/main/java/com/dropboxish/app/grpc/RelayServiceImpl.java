package com.dropboxish.app.grpc;

import com.dropboxish.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

import java.util.List;

public class RelayServiceImpl extends ApplicationServiceGrpc.ApplicationServiceImplBase{
    private List<byte[]> chunks;

    public RelayServiceImpl(List<byte[]> chunks){
        this.chunks = chunks;
    }

    @Override
    public void getFile(FileRequest request, StreamObserver<Chunk> responseObserver) {

        for(byte[] chunk : chunks) {
            responseObserver.onNext(Chunk.newBuilder()
                    .setChunk(ByteString.copyFrom(chunk))
                    .build());
        }

        responseObserver.onCompleted();
    }
}
