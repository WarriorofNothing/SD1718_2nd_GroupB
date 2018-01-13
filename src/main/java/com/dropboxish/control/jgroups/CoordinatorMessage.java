package com.dropboxish.control.jgroups;

import java.io.Serializable;

class CoordinatorMessage implements Serializable {
    //Codes - 0: Add Pool; 1: Remove Pool; 2: Put Data; 3: Delete Data
    private int code;

    private String file;
    private Shard shard;
    private int pool;

    CoordinatorMessage(int message_code){
        this.code = message_code;
    }

    CoordinatorMessage(int message_code, String file){
        this.code = message_code;
        this.file = file;
    }

    CoordinatorMessage(int message_code, String file, Shard shard, int pool){
        this.code = message_code;
        this.file = file;
        this.shard = shard;
        this.pool = pool;
    }


    int getCode() {
        return code;
    }

    String getFileName() {
        return file;
    }

    Shard getShard() {
        return shard;
    }

    int getPool() {
        return pool;
    }

    void setPool(int pool) {
        this.pool = pool;
    }
}
