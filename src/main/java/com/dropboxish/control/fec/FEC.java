package com.dropboxish.control.fec;

import com.backblaze.erasure.ReedSolomon;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FEC {
    private final static int DATA_SHARDS = 4;
    private final static int PARITY_SHARDS = 2;
    private final static int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    public static List<byte[]> encode(byte[] block){
        List<byte[]> shards = new ArrayList<>();

        final int BLOCK_SIZE = block.length + 4;
        final int SHARD_SIZE = (BLOCK_SIZE + DATA_SHARDS - 1) / DATA_SHARDS;

        int bufferSize = SHARD_SIZE * DATA_SHARDS;
        byte [] allBytes = new byte[bufferSize];
        ByteBuffer.wrap(allBytes).putInt(block.length);
        System.arraycopy(block, 0, allBytes, 4, block.length);

        byte [] [] shardsMatrix = new byte [TOTAL_SHARDS] [SHARD_SIZE];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * SHARD_SIZE, shardsMatrix[i], 0, SHARD_SIZE);
        }

        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shardsMatrix, 0, SHARD_SIZE);

        for (int i=0; i < TOTAL_SHARDS; i++)
            shards.add(shardsMatrix[i]);

        return shards;
    }

    public static byte[] decode(HashMap<Integer,byte[]> shardsList){
        final byte [] [] shards = new byte [TOTAL_SHARDS] [];
        final boolean [] shardPresent = new boolean [TOTAL_SHARDS];

        if(shardsList.size() < DATA_SHARDS)
            return null;

        int shardSize = 0;
        for(int i=0; i < TOTAL_SHARDS; i++){
            if(shardsList.containsKey(i)) {
                shardSize = shardsList.get(i).length;
                shardPresent[i] = true;
                shards[i] = shardsList.get(i).clone();
            }
        }

        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (!shardPresent[i]) {
                shards[i] = new byte [shardSize];
            }
        }

        ReedSolomon reedSolomon = ReedSolomon.create(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        byte [] allBytes = new byte [shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }

        int blockSize = ByteBuffer.wrap(allBytes).getInt();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        baos.write(allBytes,4,blockSize);

        return baos.toByteArray();
    }

    public static int getTotalShards() {
        return TOTAL_SHARDS;
    }
}
