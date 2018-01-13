package com.dropboxish.control.jgroups;

import java.io.Serializable;

class Shard implements Serializable{
    private byte[] data;
    private int index;

    Shard(byte[] data, int index){
        this.data = data.clone();
        this.index = index;
    }

    byte[] getData() {
        return data;
    }

    int getIndex() {
        return index;
    }
}
