package com.dropboxish.control.jgroups;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.lang3.SerializationUtils;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StorageController extends ReceiverAdapter{
    private JChannel channel;

    private Connection connectionGoogleSQL;
    final private List<StoragePool> storagePools = new ArrayList<>();

    public void viewAccepted(View new_view) {
        System.out.println("[INFO] Group Members: " + new_view.toString());
    }

    public void receive(Message msg) {
        CoordinatorMessage coordinatorMessage =  SerializationUtils.deserialize(msg.getBuffer());
        synchronized(storagePools) {
            if(coordinatorMessage.getCode() == 0) {                       //Add a new storage pool
                storagePools.add(new StoragePool(UUID.randomUUID().toString()));
                if(!msg.getSrc().equals(channel.getAddress()))
                    System.out.println("[INFO] Pools updated: " + storagePools.size() + " pools");
            }
            else if(coordinatorMessage.getCode() == 1) {                 //Remove a storage pool
                storagePools.remove(coordinatorMessage.getPool());
                if(!msg.getSrc().equals(channel.getAddress()))
                    System.out.println("[INFO] Pools updated: " + storagePools.size() + " pools");
            }
            else if(coordinatorMessage.getCode() == 2) {                  //Put data in the specific storage pool
                storagePools.get(coordinatorMessage.getPool()).putShard(coordinatorMessage.getFileName(), coordinatorMessage.getShard(), true);

                if(!msg.getSrc().equals(channel.getAddress()))
                    System.out.println("[INFO] Pools updated: file shard added to the pool " + (coordinatorMessage.getPool() + 1));
            }
            else if(coordinatorMessage.getCode() == 3){                 //Delete the file data
                for(StoragePool pool : storagePools)
                    pool.deleteShard(coordinatorMessage.getFileName());

                if(!msg.getSrc().equals(channel.getAddress()))
                    System.out.println("[INFO] Pools updated: file shards removed from the pools");
            }
        }
    }

    public void getState(OutputStream output) throws Exception {
        synchronized(storagePools) {
            Util.objectToStream(storagePools, new DataOutputStream(output));
        }
    }

    @SuppressWarnings("unchecked")
    public void setState(InputStream input) throws Exception {
        List<StoragePool> pools = Util.objectFromStream(new DataInputStream(input));
        synchronized(storagePools) {
            storagePools.clear();
            storagePools.addAll(pools);
        }
    }


    private void start() throws Exception {
        channel = new JChannel().setReceiver(this);
        channel.connect("ControllerGroupCluster");
        channel.getState(null, 10000);

        if(channel.getView().getCoord().equals(channel.getAddress()))        //Is the coordinator?
            coordinatorEventLoop();
        else
            eventLoop();

        channel.close();
    }

    private void connectGoogleDB() throws Exception{
        String jdbcUrl = String.format(
                "jdbc:mysql://google/%s?cloudSqlInstance=%s&"
                        + "socketFactory=com.google.cloud.sql.mysql.SocketFactory",
                "dropboxish",
                "groupb-179216:us-central1:sd-database");

        connectionGoogleSQL = DriverManager.getConnection(jdbcUrl, "grupob", "grupob");
    }

    private boolean createSQLiteTable(){
        //String url = "jdbc:sqlite:C:\\Users\\alber\\Desktop\\DB\\sqllite\\pools.db";
        String url = "jdbc:sqlite:/home/pedrokazcunha/localdb/pools.db";

        try {
            Connection connectionSQLite = DriverManager.getConnection(url);

            String sql = "CREATE TABLE IF NOT EXISTS shards (\n"
                    + " id integer PRIMARY KEY,\n"
                    + " pool_id text NOT NULL,\n"
                    + " file_name text NOT NULL,\n"
                    + " shard_index integer,\n"
                    + " shard_data blob\n"
                    + ");";

            Statement stmt = connectionSQLite.createStatement();
            stmt.execute(sql);

            connectionSQLite.close();

            return true;
        } catch (SQLException e) {
            System.out.println("[ERROR] Couldn't reload data from local DB");

            return false;
        }

    }

    private void reloadData(){
        if(!createSQLiteTable())
            return;

        storagePools.clear();

        //String url = "jdbc:sqlite:C:\\Users\\alber\\Desktop\\DB\\sqllite\\pools.db";
        String url = "jdbc:sqlite:/home/pedrokazcunha/dropboxish/localdb/pools.db";

        try {
            Connection connectionSQLite = DriverManager.getConnection(url);

            String sql = "SELECT pool_id,file_name,shard_index,shard_data FROM shards";
            Statement stmt = connectionSQLite.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while(rs.next()){
                StoragePool pool = null;

                for(StoragePool p : storagePools){
                    if(p.getID().equals(rs.getString("pool_id"))){
                        pool = p;
                    }
                }

                if (pool != null) {
                    pool.putShard(rs.getString("file_name"), new Shard(rs.getBytes("shard_data"),rs.getInt("shard_index")), true);
                } else {
                    pool = new StoragePool(rs.getString("pool_id"));
                    pool.putShard(rs.getString("file_name"), new Shard(rs.getBytes("shard_data"),rs.getInt("shard_index")), true);
                    storagePools.add(pool);
                }
            }

            connectionSQLite.close();
        } catch (SQLException e) {
            System.out.println("[ERROR] Couldn't reload data from local DB");
        }
    }

    private void ensureConsistency(){
        String url = "jdbc:sqlite:/home/pedrokazcunha/localdb/pools.db";
        //String url = "jdbc:sqlite:/home/pedrokazcunha/dropboxish/localdb/pools.db";

        try {
            Connection connectionSQLite = DriverManager.getConnection(url);

            String sql = "DROP TABLE IF EXISTS shards;";
            Statement stmt = connectionSQLite.createStatement();
            stmt.execute(sql);

            createSQLiteTable();

            for(StoragePool sp : storagePools){
                if(!sp.getMappedFiles().isEmpty()){

                    for (String file : sp.getMappedFiles()) {
                        String sql2 = "INSERT INTO shards (pool_id,file_name,shard_index,shard_data) VALUES (?,?,?,?)";
                        PreparedStatement pstmt = connectionSQLite.prepareStatement(sql2);

                        pstmt.setString(1, sp.getID());
                        pstmt.setString(2, file);
                        pstmt.setInt(3, sp.getShard(file).getIndex());
                        pstmt.setBytes(4, sp.getShard(file).getData());

                        pstmt.executeUpdate();
                    }
                }
            }

            connectionSQLite.close();
        } catch (SQLException e) {
            System.out.println("[ERROR] Couldn't reload data from local DB");
        }
    }

    private void eventLoop(){
        System.out.println("[INFO] Running with " + storagePools.size() + " pools");
        ensureConsistency();
        while(true) {

            if(channel.getView().getCoord().equals(channel.getAddress())) break;
        }

        coordinatorEventLoop();
    }

    private void coordinatorEventLoop(){
        try{
            connectGoogleDB();
            reloadData();

            if(storagePools.size() < 6){
                for(int i=0;i<6;i++){
                    storagePools.add(new StoragePool(UUID.randomUUID().toString()));
                }
            }
        } catch(Exception e){
            System.out.println("[ERROR] Couldn't connect to Database");
        } finally {
            System.out.println("[INFO] Coordinator commands (add <number_of_pool> | remove <pool_number> | pool <pool_number>)");
        }

        Server server = ServerBuilder.forPort(8082)
                .addService(new CoordinatorServiceImpl(channel, storagePools, connectionGoogleSQL))
                .build();

        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("$ "); System.out.flush();
                String line = in.readLine().toLowerCase();

                if(line.startsWith("exit")) {
                    server.shutdown();
                    connectionGoogleSQL.close();
                    break;
                }
                else if(line.startsWith("size")){
                    System.out.println("[INFO] Number of Storage Pools: " + storagePools.size());
                }
                else if(line.split(" ")[0].startsWith("add")){
                    int number_pools = 1;
                    if(line.split(" ").length > 1)
                        number_pools = Integer.valueOf(line.split(" ")[1]);

                    for(int i = 0; i < number_pools; i++){
                        CoordinatorMessage coordinatorMessage = new CoordinatorMessage(0);
                        byte[] buffer = SerializationUtils.serialize(coordinatorMessage);

                        Message msg = new Message(null, buffer);
                        channel.send(msg);
                    }

                    System.out.println("[INFO] Added " + number_pools + " new Storage Pools");
                }
                else if(line.split(" ")[0].startsWith("remove")){
                    int pool_index = storagePools.size() - 1;
                    if(line.split(" ").length > 1) {
                        pool_index = Integer.valueOf(line.split(" ")[1]) - 1;
                        if(pool_index >= storagePools.size()|| pool_index < 0) {
                            System.out.println("[ERROR] Pool out of the limits");
                            continue;
                        }
                    }

                    CoordinatorMessage coordinatorMessage = new CoordinatorMessage(1);
                    coordinatorMessage.setPool(pool_index);
                    byte[] buffer = SerializationUtils.serialize(coordinatorMessage);

                    Message msg = new Message(null, buffer);
                    channel.send(msg);

                    System.out.println("[INFO] Storage Pool " + (pool_index + 1) + " removed");
                }
                else if(line.startsWith("pool")){
                    if(line.split(" ").length > 1) {
                        int pool_index = Integer.valueOf(line.split(" ")[1]) - 1;
                        if(pool_index < storagePools.size() && pool_index >= 0) {
                            if(storagePools.get(pool_index).getMappedFiles().size() == 0)
                                System.out.println("[INFO] Storage Pool " + (pool_index + 1) + " mapping: EMPTY");
                            else {
                                System.out.println("[INFO] Storage Pool " + (pool_index + 1) + " mapping:");
                                for (String file : storagePools.get(pool_index).getMappedFiles())
                                    System.out.println("  > File: " + file + " - Shard size: " + storagePools.get(pool_index).getShard(file).getData().length);
                            }
                        } else{
                            System.out.println("[ERROR] Pool out of the limits");
                        }
                    } else{
                        System.out.println("[ERROR] Which pool?");
                    }
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        new StorageController().start();
    }
}
