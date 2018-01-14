package com.dropboxish.app;

import com.dropboxish.app.grpc.RelayServiceImpl;
import com.dropboxish.grpc.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Path("/")
public class Services {

    @GET
    @Produces(MediaType.TEXT_HTML)
    public String start(){
        return "<h1>Welcome to Dropboxish!</h1>" +
                "Choose file to upload<br>\n" +
                "<form action=\"http://localhost:8080/upload\" method=\"post\" enctype=\"multipart/form-data\">\n" +
                "\t<input name=\"file\" id=\"filename\" type=\"file\" /><br><br>\n" +
                "\t<button name=\"submit\" type=\"submit\">Upload</button>\n" +
                "</form>";
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadService(@FormDataParam("file") InputStream uploadedInputStream,
                                  @FormDataParam("file") FormDataContentDisposition fileDetails) throws Exception {
        System.out.println("** Service requested");
        System.out.println("[INFO] Trying to upload the file <" + fileDetails.getFileName() + ">");
        System.out.print("  ... ");

        if(relayToController(uploadedInputStream, fileDetails.getFileName())) {
            System.out.println("[OK] File <" + fileDetails.getFileName() + "> has been uploaded successfully");
            return Response.ok().build();
        } else {
            System.out.println("[ERROR] Upload failed");
            return Response.status(500).build();
        }
    }

    @GET
    @Path("/download")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response downloadService(@QueryParam("filename") String fileName){
        System.out.println("** Service requested");
        System.out.println("[INFO] Trying to get the file <" + fileName + ">");
        System.out.print("  ... ");

        byte[] fileInBytes = getFileFromController(fileName);

        if(fileInBytes.length == 0){
            System.out.println("[ERROR] Couldn't get the file");
            return Response.status(500).build();
        } else {
            System.out.println("[OK] File <" + fileName + "> has been sent successfully");
            return Response.ok(fileInBytes, MediaType.APPLICATION_OCTET_STREAM)
                    .header("Content-Disposition", "attachment; filename=\"" + fileName + "\"")
                    .build();
        }
    }

    @POST
    @Path("/delete")
    @Consumes(MediaType.TEXT_PLAIN)
    public Response deleteService(String fileName) {
        System.out.println("** Service requested");
        System.out.println("[INFO] Trying to delete the file <" + fileName + ">");
        System.out.print("  ... ");

        if(deleteFileFromController(fileName)) {
            System.out.println("[OK] File <" + fileName + "> has been deleted successfully");
            return Response.ok().build();
        } else{
            System.out.println("[ERROR] Couldn't delete the file");
            return Response.status(500).build();
        }
    }

    @GET
    @Path("/list")
    @Consumes(MediaType.TEXT_PLAIN)
    public String listFilesService(@QueryParam("pattern") String pattern){
        System.out.println("** Service requested");
        System.out.println("[INFO] Getting the files list");
        System.out.print("  ... ");

        JsonObject msg = new JsonObject();

        try {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("10.128.0.2", 8082)
                    .usePlaintext(true)
                    .build();

            ApplicationServiceGrpc.ApplicationServiceBlockingStub stub = ApplicationServiceGrpc.newBlockingStub(channel);

            Iterator<ListResponse> filesList = stub.getList(RelayRequest.newBuilder()
                    .setPattern(pattern)
                    .build());

            JsonArray jsonArray = new JsonArray();
            while (filesList.hasNext()){
                JsonObject fileObj = new JsonObject();
                ListResponse response = filesList.next();

                if(response.getFileName().equals("ERROR")) {
                    System.out.println("[ERROR] No connection to DB");
                    return "ERROR";
                }

                fileObj.addProperty("name", response.getFileName());
                fileObj.addProperty("size", response.getFileSize());

                jsonArray.add(fileObj);
            }

            channel.shutdown();

            msg.add("files", jsonArray);

        } catch(Exception e){
            System.out.println("[ERROR] Storage group unavailable");
            e.printStackTrace();
            return "ERROR";
        }

        System.out.println("[OK] List has been sent");
        return msg.toString();
    }

    private boolean deleteFileFromController(String fileName){
        try {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("10.128.0.2", 8082)
                    .usePlaintext(true)
                    .build();

            ApplicationServiceGrpc.ApplicationServiceBlockingStub stub = ApplicationServiceGrpc.newBlockingStub(channel);

            RelayResponse controllerFeedback = stub.relayDelete(RelayRequest.newBuilder()
                    .setFileName(fileName)
                    .build());

            boolean isStatusOk = controllerFeedback.getStatus().equals("OK");

            channel.shutdown();

            return isStatusOk;
        } catch (Exception e){

            return false;
        }
    }

    private byte[] getFileFromController(String fileName){
        try {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("10.128.0.2", 8082)
                    .usePlaintext(true)
                    .build();

            ApplicationServiceGrpc.ApplicationServiceBlockingStub stub = ApplicationServiceGrpc.newBlockingStub(channel);
            Iterator<Chunk> chunkData = stub.getFile(FileRequest.newBuilder()
                    .setFileName(fileName)
                    .build());

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while (chunkData.hasNext()) {
                byte[] chunk_received = chunkData.next().getChunk().toByteArray();
                baos.write(chunk_received, 0, chunk_received.length);
            }

            channel.shutdown();

            return baos.toByteArray();

        } catch (Exception e){
            return new byte[0];
        }
    }

    private boolean relayToController(InputStream uploadedInputStream, String fileName){
        try {
            List<byte[]> chunks = new ArrayList<>();

            ByteArrayOutputStream baos;
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            byte[] buffer = new byte[4096];
            int read_bytes;

            while ((read_bytes = uploadedInputStream.read(buffer)) != -1) {
                baos = new ByteArrayOutputStream();
                baos.write(buffer, 0, read_bytes);
                md.update(buffer, 0, read_bytes);

                chunks.add(baos.toByteArray());
            }

            byte[] mdbytes = md.digest();

            StringBuilder hex_hash = new StringBuilder();
            for (byte mdbyte : mdbytes) {
                hex_hash.append(Integer.toHexString(0xFF & mdbyte));
            }

            Server server = ServerBuilder.forPort(8081)
                    .addService(new RelayServiceImpl(chunks))
                    .build();


            server.start();

            ManagedChannel channel = ManagedChannelBuilder.forAddress("10.128.0.2", 8082)
                    .usePlaintext(true)
                    .build();

            ApplicationServiceGrpc.ApplicationServiceBlockingStub stub = ApplicationServiceGrpc.newBlockingStub(channel);

            RelayResponse controllerFeedback = stub.relayFile(RelayRequest.newBuilder()
                    .setFileName(fileName)
                    .setFileHash(hex_hash.toString())
                    .build());

            boolean isStatusOk = controllerFeedback.getStatus().equals("OK");

            server.shutdown();
            channel.shutdown();

            return isStatusOk;

        } catch (Exception e){
            return false;
        }
    }
}
