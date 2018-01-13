package com.dropboxish.control.jgroups;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;

class StoragePool implements Serializable{
    private HashMap<String, Shard> blocksMapping = new HashMap<>();
    private String id;

    StoragePool(String id){
        this.id = id;
    }

    boolean putShard(String file, Shard shard, boolean reload){
        if(reload) {
            blocksMapping.put(file, shard);
            return true;
        } else{
            blocksMapping.put(file, shard);
            return storeShard(file,shard);
        }
    }

    Shard getShard(String file){
        return blocksMapping.get(file);
    }

    boolean deleteShard(String file){
        return blocksMapping.remove(file) != null && removeShard(file);
    }

    Set<String> getMappedFiles(){
        return blocksMapping.keySet();
    }

    String getID() {
        return id;
    }

    private boolean storeShard(String file, Shard shard){
        String url = "jdbc:sqlite:C:\\Users\\alber\\Desktop\\DB\\sqllite\\pools.db";

        try {
            Connection connectionSQLite = DriverManager.getConnection(url);

            String sql = "INSERT INTO shards (pool_id,file_name,shard_index,shard_data) VALUES (?,?,?,?)";
            PreparedStatement pstmt = connectionSQLite.prepareStatement(sql);

            pstmt.setString(1, id);
            pstmt.setString(2, file);
            pstmt.setInt(3, shard.getIndex());
            pstmt.setBytes(4, shard.getData());

            pstmt.executeUpdate();
            return true;

        } catch (SQLException e) {
            System.out.println("[ERROR] Couldn't store into local DB");
            return false;
        }
    }

    private boolean removeShard(String file){
        String url = "jdbc:sqlite:C:\\Users\\alber\\Desktop\\DB\\sqllite\\pools.db";

        try {
            Connection connectionSQLite = DriverManager.getConnection(url);

            String sql = "DELETE FROM shards WHERE pool_id = ? AND file_name = ?";
            PreparedStatement pstmt = connectionSQLite.prepareStatement(sql);

            pstmt.setString(1, id);
            pstmt.setString(2,file);

            pstmt.executeUpdate();
            return true;

        } catch (SQLException e) {
            System.out.println("[ERROR] Couldn't delete from local DB");
            return false;
        }
    }
}
