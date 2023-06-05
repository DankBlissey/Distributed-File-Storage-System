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
                    break;
                case "STORE":
                    System.out.println("Client store request");
                    break;
            }
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
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
