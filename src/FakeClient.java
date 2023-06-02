import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FakeClient {
    public static void main(String [] args) {
        Socket socket = null;
        byte[] fileBytes = null;
        try {
            fileBytes = Files.readAllBytes(Paths.get("C:/Users/John/Documents/TestFile.txt"));
        } catch (Exception e) {
            System.err.println("Error" + e);
        }
        try {
            InetAddress address = InetAddress.getLocalHost();
            socket = new Socket(address, Integer.parseInt(args[0]));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            assert fileBytes != null;
            socket.getOutputStream().write(fileBytes);

        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (socket != null)
                try { socket.close(); } catch (IOException e) { System.err.println("error: " + e); }
        }
    }
}
