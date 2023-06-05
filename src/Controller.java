import java.net.ServerSocket;
import java.io.File;
import java.io.*;
import java.net.*;
import java.util.ArrayList;

public class Controller {
    static ServerSocket cport;
    static Integer R;
    static Integer timeout;
    static Integer rebalancePeriod;
    static ArrayList<FileIndex> Index;
    static ArrayList<Socket> DstoreList;

    public static void main(String[] args) {
        try {
            cport = new ServerSocket(Integer.parseInt(args[0]));
            R = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            rebalancePeriod = Integer.parseInt(args[3]);

            try {
                assert cport != null : "Connection is null";
                new Thread(new controllerThread(cport.accept())).start();
                System.out.println("Client connected:");
            } catch (Exception e) {
                System.err.println("Error: " + e);
            }
        } catch (Exception e) {
            System.err.println("Issues with Dstore setup: " + e);
        }

    }

    public static void receive(Socket c) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream()));
            String line = in.readLine();
            String[] lines = line.split(" ");
            switch(lines[0]) {
                case "JOIN":
                    System.out.println("Dstore connected with port " + lines[1]);
                    DstoreList.add(c);
                    //re-balance could go here?
                    break;
                case "STORE":
                    System.out.println("Client store request");
                    if(DstoreList.size() >= R) {
                        if(!isFileInIndex(lines[1])) {
                            //store
                        } else {
                            System.err.println("Error with storage request: file already exists");
                            PrintWriter out = new PrintWriter(c.getOutputStream(), true);
                            out.println("ERROR_FILE_ALREADY_EXISTS");
                        }
                    } else {
                        System.err.println("Error with storage request: not enough Dstores");
                        PrintWriter out = new PrintWriter(c.getOutputStream(), true);
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                    }
                    break;
            }
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
    }

    public static boolean isFileInIndex(String fileName) {
        boolean answer = false;
        for(FileIndex e : Index) {
            if (e.getFileName().equals(fileName)) {
                answer = true;
                break;
            }
        }
        return answer;
    }

    static class controllerThread implements Runnable {

        Socket connector;

        controllerThread(Socket c) {
            connector = c;
        }

        public void run() {
            receive(connector);
        }
    }
}
