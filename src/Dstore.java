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
            while(true) {
                try {
                    Socket client = port.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line;
                    while((line = in.readLine()) != null) System.out.println(line+" received");
                    client.close();
                } catch(Exception e) { System.err.println("error: " + e); }
            }


        } catch (Exception e) {
            System.err.println("Issues with Dstore setup" + e);
        }
    }

}
