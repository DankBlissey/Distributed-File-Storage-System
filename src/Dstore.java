import java.io.*;
import java.net.*;
import java.nio.file.Path;

public class Dstore {
    public static void main (String [] args) {
        ServerSocket port = null;
        ServerSocket cport = null;
        Integer timeout = null;
        String fileFolderTxt = null;
        Socket client = null;
        try {
            port = new ServerSocket(Integer.parseInt(args[0]));
            cport = new ServerSocket(Integer.parseInt(args[1]));
            timeout = Integer.parseInt(args[2]);
            fileFolderTxt = args[3];
                /*
                try {
                    Socket client = port.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line;
                    while((line = in.readLine()) != null) System.out.println(line+" received");
                    client.close();
                } catch(Exception e) { System.err.println("error: " + e); }

                 */
        } catch (Exception e) {
            System.err.println("Issues with Dstore setup: " + e);
        }


        try {
            client = port.accept();
        } catch (Exception e) {
            System.err.println("Socket Accept failed: ");
        }
        StoreFile(client, fileFolderTxt);
    }

    public static void StoreFile(Socket client, String fileFolderTxt) {
        try {
            System.out.println("Client Connected: " + client.getInetAddress().getHostAddress());
            InputStream inputStream = client.getInputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            String fileName = "recievedFile.txt";
            Path destinationPath = Path.of(fileFolderTxt + fileName);
            FileOutputStream fileOutputStream = new FileOutputStream(destinationPath.toString());

            while ((bytesRead = inputStream.readNBytes(buffer, 0, buffer.length)) > 0) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }

            fileOutputStream.close();

            System.out.println("File recieved and saved to: " + fileFolderTxt);
        } catch (Exception e) {
            System.err.println("error: " + e);
        }
    }

    public static void ReceiveRequest() {

    }


}
