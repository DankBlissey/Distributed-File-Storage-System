import java.net.ServerSocket;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class Controller {
    static ServerSocket cport;
    static Integer R;
    static Integer timeout;
    static Integer rebalancePeriod;
    //static ArrayList<FileIndex> Index;
    static HashMap<String, FileIndex> Index;
    static HashMap<String, Socket> DstoreList;


    public static synchronized void addToIndex(FileIndex f) {
        Index.put(f.getFileName(),f);
    }

    public static synchronized void updateIndexStatus(String fileName, String status) {
        try {
            Index.get(fileName).setStatus(status);
        } catch (Exception e) {
            System.err.println("Typo in status update for index");
        }
    }

    public static synchronized ArrayList<FileIndex> getIndexFiles() {
        return new ArrayList<>(Index.values());
    }

    public static synchronized HashMap<String, FileIndex> getIndex() {
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
                    c.setSoTimeout(timeout);
                    //re-balance could go here?
                    break;
                case "STORE":
                    System.out.println("Client store request");
                    if(getDstoreList().size() >= R) {
                        if(!isFileInIndex(lines[1])) {
                            List<String> Dgo = selectDstores();
                            String fileName = lines[1];
                            Integer fileSize = Integer.parseInt(lines[2]);
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
                    ArrayList<FileIndex> index = new ArrayList<>(getIndexFiles());
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

    public static synchronized boolean isFileInIndex(String fileName) {
        return getIndex().containsKey(fileName);
    }

    public static void allocateDstores(Socket c, String fileName, List<String> Dgo) {
        try {
            PrintWriter out = new PrintWriter(c.getOutputStream(), true);
            String DstoresToGo = "STORE_TO " + String.join(" ", Dgo);
            HashMap<String, Socket> Dstores = new HashMap<>(getDstoreList());
            out.println(DstoresToGo);

            List<String> outPutScraper = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch countDownLatch = new CountDownLatch(R);
            List<Thread> workers = new ArrayList<>();
            ArrayList<String> target = new ArrayList<>();
            for(String s : Dgo) {
                workers.add(new Thread(new Worker(s, Dstores.get(s), fileName, outPutScraper, countDownLatch)));
                target.add("Counted down");
            }
            workers.forEach(Thread::start);
            boolean completed = countDownLatch.await(timeout * 2, TimeUnit.MILLISECONDS);
            outPutScraper.add("Latch released");
            target.add("Latch released");

            if(outPutScraper.containsAll(target)) {
                out.println("STORE_COMPLETE");
                c.close();
            } else {

            }
        } catch (Exception e) {
            System.err.println("Error : " + e);
        }
    }

    public static List<String> selectDstores() {
        List<String> Dstores;
        List<String> DstoresWithFiles;
        synchronized (Controller.class) {
            Dstores = new ArrayList<>(getDstoreList().keySet());
            DstoresWithFiles = new ArrayList<>(getLocationsWithLeastFiles());
        }
        Dstores.removeAll(DstoresWithFiles);
        int n = R - Dstores.size();
        Dstores.addAll(DstoresWithFiles.subList(0, Math.min(n, DstoresWithFiles.size())));
        return Dstores;
    }

    public static List<String> getLocationsWithLeastFiles() {
        HashMap<String, Integer> storageCounts = new HashMap<>();
        ArrayList<FileIndex> index = new ArrayList<>(getIndexFiles());
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

    static class Worker implements Runnable {
        private List<String> outputScraper;
        private CountDownLatch countDownLatch;
        private String port;
        private Socket connector;
        private String fileName;

        public Worker(String port, Socket c, String s, List<String> outputScraper, CountDownLatch countDownLatch) {
            this.port = port;
            this.connector = c;
            this.fileName = s;
            this.outputScraper = outputScraper;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(connector.getInputStream()));
                String line;
                line = in.readLine();
                if(!line.equals("STORE_ACK " + fileName)) {
                    System.err.println("Thread: Storage acceptance message incorrect");
                    System.err.println("Dstore non-functional, removing Dstore");
                    //remove Dstore method goes here
                } else {
                    outputScraper.add("Counted down");
                    countDownLatch.countDown();
                }
            } catch (Exception e) {
                System.err.println("Error: " + e);
                outputScraper.add("failed for Dstore: " + port);
                countDownLatch.countDown();
            }
        }
    }

    /*
    static class DstoreReplyJob implements Callable<Boolean> {
        Socket connector;
        String fileName;

        public DstoreReplyJob(Socket c, String s) {
            this.connector = c;
            this.fileName = s;
        }

        @Override
        public Boolean call() throws Exception {
            BufferedReader in = new BufferedReader(new InputStreamReader(connector.getInputStream()));
            String line;
            line = in.readLine();
            if(!line.equals("STORE_ACK " + fileName)) {
                System.err.println("Thread: Storage acceptance message incorrect");
                return false;
            } else {
                return true;
            }
        }
    }
     */
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
