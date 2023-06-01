import java.io.*;
import java.net.*;

public class Dstore {
    public static void main (String [] args) {
        ServerSocket port = null;
        ServerSocket cport = null;
        Integer timeout = null;
        String fileFolderTxt = null;
        File fileFolder = null;
        try {
            port = new ServerSocket(Integer.parseInt(args[0]));
            cport = new ServerSocket(Integer.parseInt(args[1]));
            timeout = Integer.parseInt(args[2]);
            fileFolderTxt = args[3];
            try {
                fileFolder = new File(fileFolderTxt);
            } catch (Exception c) {
                System.err.println("error" + c);
            }

        } catch (Exception e) {
            System.err.println("Issues with Dstore setup" + e);
        }
    }

}
