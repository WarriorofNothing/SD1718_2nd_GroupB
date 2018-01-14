package com.dropboxish.client;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.cli.*;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class Client {

    public static void main(String[] args) throws ParseException{

        Client clint = new Client();
        clint.start();
    }

    private void start() throws ParseException{
        Scanner in = new Scanner(System.in);

        Options options = new Options();
        options.addOption("h", "help", false, "help to use Dropboxish");
        options.addOption("e", "exit", false , "exit Dropboxish");
        options.addOption("l", "list", false, "list all files stored");

        Option uploadOption = new Option("up", "upload", true, "upload a file" );
        uploadOption.setArgName("<file name>");
        uploadOption.setOptionalArg(true);
        options.addOption(uploadOption);

        Option downloadOption = new Option("down", "download", true, "download a file" );
        downloadOption.setArgName("<file name>");
        downloadOption.setOptionalArg(true);
        options.addOption(downloadOption);

        Option deleteOption = new Option("del", "delete", true, "delete a file" );
        deleteOption.setArgName("<file name>");
        deleteOption.setOptionalArg(true);
        options.addOption(deleteOption);

        Option searchOption = new Option("s", "search", true, "search for files" );
        searchOption.setArgName("<pattern>");
        searchOption.setOptionalArg(true);
        options.addOption(searchOption);

        System.out.println("Welcome to Dropboxish");
        System.out.println("If it's your first time here type -help for help.");

        String[] args = getCommandLine(in);
        CommandLine command = new DefaultParser().parse(options, args);
        while(!command.hasOption("exit")) {

            if(command.getArgs().length == 0) {
                if (command.hasOption("download")) {
                    if(command.getOptionValue("download") == null)
                        System.out.println("Missing argument: type -help for Help. ");
                    else {
                        download(command.getOptionValue("download"));
                    }
                } else if(command.hasOption("upload")){
                    if(command.getOptionValue("upload") == null)
                        System.out.println("Missing argument: type -help for Help. ");
                    else {
                        upload(command.getOptionValue("upload"));
                    }
                } else if(command.hasOption("delete")){
                    if(command.getOptionValue("delete") == null)
                        System.out.println("Missing argument: type -help for Help. ");
                    else {
                        delete(command.getOptionValue("delete"));
                    }
                } else if(command.hasOption("search")){
                    if(command.getOptionValue("search") == null)
                        System.out.println("Missing argument: type -help for Help. ");
                    else {
                        listFiles(command.getOptionValue("search"));
                    }
                } else if (command.hasOption("help")) {
                    for (Option o : options.getOptions()) {
                        if (o.hasArg()) {
                            System.out.println("    -" + o.getLongOpt() + " " + o.getArgName() + ": " + o.getDescription());
                        } else
                            System.out.println("    -" + o.getLongOpt() + ": " + o.getDescription());
                    }
                } else if(command.hasOption("list")){
                    listFiles("");
                } else {
                    System.out.println("Unknown command: type -help for Help.");
                }
            }
            else System.out.println("Unknown command: type -help for Help.");

            args = getCommandLine(in);
            command = new DefaultParser().parse(options, args);
        }
    }

    private String[] getCommandLine(Scanner in){
        System.out.print("$ ");
        String line = in.nextLine();
        return line.split(" ");
    }

    private void upload(String fileName){
        Thread uploadThread = new Thread( () -> {
            try {
                CloseableHttpClient httpClient = HttpClients.createDefault();
                HttpPost uploadFile = new HttpPost("http://104.198.245.139:8080/upload");
                MultipartEntityBuilder builder = MultipartEntityBuilder.create();
                builder.addTextBody("field1", "yes", ContentType.TEXT_PLAIN);

                File file = new File("C:\\Users\\alber\\Desktop\\imagens\\" + fileName);
                builder.addBinaryBody(
                        "file",
                        new FileInputStream(file),
                        ContentType.APPLICATION_OCTET_STREAM,
                        file.getName()
                );

                HttpEntity multipart = builder.build();
                uploadFile.setEntity(multipart);

                try (CloseableHttpResponse response = httpClient.execute(uploadFile)) {
                    if (response.getStatusLine().getStatusCode() == 200) {
                        System.out.println("File <" + fileName + "> uploaded.");
                    } else
                        System.out.println("Sorry! Upload Failed.");
                }
            } catch (FileNotFoundException e) {
                System.out.println("Couldn't find the file <" + fileName + ">.");
            } catch (IOException e) {
                System.out.println("Sorry! Service is unavailable at this moment.");
            }
        });

        uploadThread.start();
    }

    private void download(String fileName){
        Thread downloadThread = new Thread( () -> {
            try {
                File file = new File("C:\\Users\\alber\\Downloads\\Download_Trab" + fileName);

                CloseableHttpClient client = HttpClients.createDefault();
                String query = "?filename=" + fileName;
                try (CloseableHttpResponse response = client.execute(new HttpGet("http://104.198.245.139/download" + query))) {
                    if (response.getStatusLine().getStatusCode() == 500) {
                        System.out.println("Sorry! Download Failed.");
                        return;
                    }

                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try (FileOutputStream outstream = new FileOutputStream(file)) {
                            entity.writeTo(outstream);
                            System.out.println("File <" + fileName + "> downloaded.");
                        }
                    }
                }
            } catch (IOException e) {
                System.out.println("Sorry! Service is unavailable at this moment.");
            }
        });

        downloadThread.start();
    }

    private void delete(String fileName){
        Thread deleteFileThread = new Thread( () -> {
            try {
                CloseableHttpClient client = HttpClients.createDefault();

                HttpPost request = new HttpPost("http://104.198.245.139:8080/delete");
                StringEntity params = new StringEntity(fileName);
                request.addHeader("content-type", "text/plain");
                request.setEntity(params);

                try (CloseableHttpResponse response = client.execute(request)) {
                    if (response.getStatusLine().getStatusCode() == 200)
                        System.out.println("File <" + fileName + "> deleted!");
                    else
                        System.out.println("Sorry! Couldn't delete the file <" + fileName + ">");
                }
            } catch (IOException e) {
                System.out.println("Sorry! Couldn't delete the file <" + fileName + ">");
            }
        });

        deleteFileThread.start();
    }

    private void listFiles(String pattern){
        try {
            String query = "?pattern=" + pattern;
            URL url = new URL("http://104.198.245.139:8080/list/" + query);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            InputStreamReader isr = new InputStreamReader(conn.getInputStream());
            BufferedReader br = new BufferedReader(isr);

            String line;
            StringBuilder json_response = new StringBuilder();

            while ((line = br.readLine()) != null) {
                json_response.append(line);
            }

            br.close();

            if (!json_response.toString().equals("ERROR")) {
                JsonObject jsonObj = new JsonParser().parse(json_response.toString()).getAsJsonObject();
                JsonArray filesJsonArray = jsonObj.get("files").getAsJsonArray();

                for (int i = 0; i < filesJsonArray.size(); i++) {
                    JsonObject fileObj = filesJsonArray.get(i).getAsJsonObject();
                    System.out.println("  > " + fileObj.get("name").getAsString() + " (" + fileObj.get("size").getAsLong() + " bytes)");
                }
            } else System.out.println("Sorry! Service is unavailable at this moment.");
        } catch (IOException e) {
            System.out.println("Sorry! Service is unavailable at this moment.");
        }
    }
}
