import java.net.ServerSocket;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Controller {
    static ServerSocket cport;
    static Integer R;
    static Integer timeout;
    static Integer rebalancePeriod;
    static ArrayList<FileIndex> Index;
    static HashMap<String, Socket> DstoreList;


    public static synchronized void addToIndex(FileIndex f) {
        Index.add(f);
    }

    public static synchronized ArrayList<FileIndex> getIndex() {
        return Index;
    }

    public static synchronized void addToDstoreList(String s, Socket c)  {
        DstoreList.put(s,c);
    }

    public static synchronized Socket getDstore(String s) {
        return DstoreList.get(s);
    }

    public static synchronized HashMap<String, Socket> getDstoreList() {
        return DstoreList;
    }

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
                    addToDstoreList(lines[1], c);
                    //re-balance could go here?
                    break;
                case "STORE":
                    System.out.println("Client store request");
                    if(getDstoreList().size() >= R) {
                        if(!isFileInIndex(lines[1])) {
                            String fileName = lines[1];
                            Integer fileSize = Integer.parseInt(lines[2]);
                            List<String> Dgo = selectDstores();
                            addToIndex(new FileIndex(fileName, fileSize, Dgo));
                            allocateDstores(c, fileName, Dgo);
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
                case "LIST":
                    PrintWriter out = new PrintWriter(c.getOutputStream());
                    ArrayList<FileIndex> index = new ArrayList<>(getIndex());
                    for(FileIndex f : index) {
                        out.print(f.getFileName() + " ");
                    }
                    out.flush();
                    break;
            }
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
    }

    public static boolean isFileInIndex(String fileName) {
        boolean answer = false;
        for(FileIndex e : getIndex()) {
            if (e.getFileName().equals(fileName)) {
                answer = true;
                break;
            }
        }
        return answer;
    }

    public static ArrayList<String> allocateDstores(Socket c, String fileName, List<String> Dgo) {
        try {
            PrintWriter out = new PrintWriter(c.getOutputStream(), true);
            String DstoresToGo = "STORE_TO " + String.join(" ", Dgo);
            HashMap<String, Socket> Dstores = new HashMap<>(getDstoreList());
            out.println(DstoresToGo);
            ExecutorService service = Executors.newFixedThreadPool(R);
            HashMap<String, Future<Boolean>> futures = new HashMap<>();
            for(String s : Dgo) {
                futures.put(s,service.submit(new DstoreReplyJob(Dstores.get(s), fileName)));
            }
            ArrayList<String> FailedDstores = new ArrayList<>();
            futures.forEach((key, value) -> {
                Boolean allM = true;
                try {
                    if(value.get(timeout + 50, TimeUnit.MILLISECONDS).equals(false)) {
                        System.err.println("Message from Dstore" + key + " incorrect");
                        FailedDstores.add(key);
                        allM = false;
                    }
                    if(allM.equals(true)) {
                        out.println("STORE_COMPLETE");
                    }
                } catch (InterruptedException | TimeoutException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    if(e.getCause() instanceof SocketTimeoutException) {
                        System.err.println("Timeout occurred with Dstore " + key);
                        FailedDstores.add(key);
                    }
                }
            });
            try {
                c.close();
            } catch (Exception e) {
                System.err.println("Error with closing client socket: " + e);
            }
            return FailedDstores;
        } catch (Exception e) {
            System.err.println("Error : " + e);
            return null;
        }
    }

    public static List<String> selectDstores() {
        List<String> Dstores = new ArrayList<>(getDstoreList().keySet());
        List<String> DstoresWithFiles = new ArrayList<>(getLocationsWithLeastFiles());
        Dstores.removeAll(DstoresWithFiles);
        int n = R - Dstores.size();
        Dstores.addAll(DstoresWithFiles.subList(0, Math.min(n, DstoresWithFiles.size())));
        return Dstores;
    }

    public static List<String> getLocationsWithLeastFiles() {
        HashMap<String, Integer> storageCounts = new HashMap<>();
        ArrayList<FileIndex> index = new ArrayList<>(getIndex());
        for (FileIndex entry : index) {
            List<String> locations = entry.getDstoreAllocation();
            for (String location : locations) {
                storageCounts.put(location, storageCounts.getOrDefault(location, 0) + entry.getFileSize());
            }
        }
        List<String> sortedLocations = new ArrayList<>(storageCounts.keySet());
        sortedLocations.sort(Comparator.comparingInt(storageCounts::get));
        return sortedLocations;
    }

    static class DstoreReplyJob implements Callable<Boolean> {
        Socket connector;
        String fileName;

        public DstoreReplyJob(Socket c, String s) {
            this.connector = c;
            this.fileName = s;
        }

        @Override
        public Boolean call() throws Exception {
            connector.setSoTimeout(timeout);
            BufferedReader in = new BufferedReader(new InputStreamReader(connector.getInputStream()));
            String line;
            line = in.readLine();
            connector.setSoTimeout(0);
            if(!line.equals("STORE_ACK " + fileName)) {
                System.err.println("Thread: Storage acceptance message incorrect");
                return false;
            } else {
                return true;
            }
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
