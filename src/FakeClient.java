import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FakeClient {
    public static void main(String [] args) {
        Socket socket = null;
        byte[] fileBytes = null;
        String path = args[1];
        String name = args[2];
        try {
            fileBytes = Files.readAllBytes(Paths.get(path));
        } catch (Exception e) {
            System.err.println("Error" + e);
        }
        try {
            InetAddress address = InetAddress.getLocalHost();
            socket = new Socket(address, Integer.parseInt(args[0]));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            assert fileBytes != null;
            out.println("STORE " + name + " 1Kb");
            System.out.println("Requesting to store file");
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String reply;
            reply = in.readLine();
                if(reply.equals("ACK")) {
                    System.out.println("Request acceptance received, sending file data");
                    //Thread.sleep(2000);
                    socket.getOutputStream().write(fileBytes);
                    System.out.println("Data Sent");
                }

        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (socket != null)
                try { socket.close(); } catch (IOException e) { System.err.println("error: " + e); }
        }
    }
}
