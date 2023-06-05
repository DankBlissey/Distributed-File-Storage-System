import java.net.ServerSocket;
import java.io.File;
import java.io.*;
import java.net.*;

public class Controller {
    static ServerSocket cport;
    static Integer R;
    static Integer timeout;
    static Integer rebalancePeriod;
    static Socket connection;

    public static void main(String[] args) {
        try {
            cport = new ServerSocket(Integer.parseInt(args[0]));
            R = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            rebalancePeriod = Integer.parseInt(args[3]);

            try {
                assert cport != null : "Connection is null";
                connection = cport.accept();

            } catch (Exception e) {
                System.err.println("Error: " + e);
            }
        } catch (Exception e) {
            System.err.println("Issues with Dstore setup: " + e);
        }

    }

    public static void recieve() {

    }
}
