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
    static HashMap<String, FileIndex> Index = new HashMap<>();
    static HashMap<String, DStoreI> DstoreList = new HashMap<>();
    static HashMap<DStoreI, String> ReverseDstoreList = new HashMap<>();
    static ThreadLocal<Integer> indexToStore = new ThreadLocal<>();
    static final Object rebalanceObj = new Object();
    static final Object rebalanceLock = new Object();
    static final Object rebalanceStart = new Object();
    static List<DstoreFileList> rebalanceDstoreFiles = new ArrayList<>();
    static CountDownLatch rebalanceCd;

    public static synchronized void removeDstoreFromFileIndex(String port) {
        for(FileIndex f : getIndexFiles()) {
            if(f.getDstoreAllocation().contains(port)) {
                f.removeDstoreFromAllocation(port);
            }
        }
    }

    public static synchronized void removeDstoreFromOneFileIndex(String fileName, String port) {
        Index.get(fileName).removeDstoreFromAllocation(port);
    }

    public static synchronized void addDstoreToFileIndex(String fileName, String port) {
        Index.get(fileName).addDstoreToAllocation(port);
    }

    public static CountDownLatch getRebalanceCountDownLatch() {
        return rebalanceCd;
    }

    public static synchronized void setRebalanceCountDownLatch(int i) {
        rebalanceCd = new CountDownLatch(i);
    }

    public static void countDownRebalance() {
        rebalanceCd.countDown();
    }

    public static synchronized void addRebalanceDstoreFiles(DstoreFileList s) {
        rebalanceDstoreFiles.add(s);
    }

    public static synchronized void wipeRebalanceDstoreFiles() {
        rebalanceDstoreFiles = new ArrayList<>();
    }

    public static synchronized List<DstoreFileList> getRebalanceDstoreFiles() {
        return rebalanceDstoreFiles;
    }

    public static void setIndexToStore(int value) {
        indexToStore.set(value);
    }

    public static int getIndexToStore() {
        return indexToStore.get();
    }

    public static synchronized List<String> getIndexDstores(String fileName) {
        return Index.get(fileName).getDstoreAllocation();
    }

    public static synchronized void addToIndex(FileIndex f) {
        Index.put(f.getFileName(),f);
    }

    public static synchronized void removeFromIndex(String fileName) {
        Index.remove(fileName);
    }

    public static synchronized void updateIndexStatus(String fileName, String status) {
        try {
            Index.get(fileName).setStatus(status);
        } catch (Exception e) {
            System.err.println("Typo in status update for index");
        }
    }

    public static synchronized void countdownIndex(String fileName) {
        Index.get(fileName).getCountDownLatch().countDown();
    }

    public static synchronized CountDownLatch getLatch(String fileName) {
        return Index.get(fileName).getCountDownLatch();
    }

    public static synchronized void setLatch(String fileName, int i) {
        Index.get(fileName).setCountDownLatch(i);
    }

    public static synchronized ArrayList<FileIndex> getIndexFiles() {
        return new ArrayList<>(Index.values());
    }

    public static synchronized HashMap<String, FileIndex> getIndex() {
        return Index;
    }

    public static synchronized void addToDstoreList(String s, DStoreI store)  {
        DstoreList.put(s,store);
        ReverseDstoreList.put(store,s);
    }

    public static synchronized void removeDStoreList(String s) {
        ReverseDstoreList.remove(DstoreList.get(s));
        DstoreList.remove(s);
    }

    public static synchronized void removeDStoreList(DStoreI i) {
        DstoreList.remove(ReverseDstoreList.get(i));
        ReverseDstoreList.remove(i);
    }

    public static synchronized DStoreI getDstoreI(String s) {
        return DstoreList.get(s);
    }

    public static synchronized String getDStoreIPort(DStoreI i ) {
        return ReverseDstoreList.get(i);
    }

    public static synchronized HashMap<String, DStoreI> getDstoreList() {
        return DstoreList;
    }

    public static synchronized FileIndex getIndexFile(String fileName) {
        return Index.get(fileName);
    }

    public static void main(String[] args) {
        try {
            cport = new ServerSocket(Integer.parseInt(args[0]));
            R = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            rebalancePeriod = Integer.parseInt(args[3]);
            System.out.println("Starting");
            Thread rebThread = new Thread(new RebalanceThread());
            rebThread.setPriority(10);
            rebThread.start();

            while(true) {
                try {
                    System.out.println("New connection being accepted");
                    Socket c = cport.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(c.getInputStream()));
                    PrintWriter out = new PrintWriter(c.getOutputStream());
                    new Thread(new controllerThread(c, in, out)).start();
                    System.out.println("Client connected:");
                } catch (Exception e) {
                    System.err.println("Error: " + e);
                }
            }
        } catch (Exception e) {
            System.err.println("Issues with Dstore setup: " + e);
        }

    }

    public static void receive(Socket c, BufferedReader in, PrintWriter out) {
        try {
            System.out.println("Reading line");
            String line;
            while((line = in.readLine()) != null) {
                System.out.println("Line read");
                String[] lines = line.split(" ");
                synchronized (rebalanceStart) {
                    System.out.println("Waiting for rebalance to finish if it is running");
                    //waits until rebalance has finished before starting to serve a client request
                }
                switch (lines[0]) {
                    case "JOIN" -> {
                        System.out.println("Dstore connected with port " + lines[1]);
                        //if(c.isClosed()) {
                        //    System.out.println("socket closed before adding to dstorelist");
                        //}
                        DStoreI store = new DStoreI(c, in, out);
                        addToDstoreList(lines[1], store);
                        //if(c.isClosed()) {
                        //    System.out.println("socket closed after adding to dstorelist");
                        //}
                        synchronized (rebalanceObj) {
                            rebalanceObj.notify();
                        }
                        recieveDstoreMsg(store);
                    }

                    case "STORE" -> {
                        System.out.println("Client store request");
                        synchronized (rebalanceLock) {
                            //this means that rebalance will have to wait for store to complete before it may start
                            synchronized (Controller.class) {
                                if (getDstoreList().size() >= R) {
                                    if ((!isFileInIndex(lines[1])) || ((isFileInIndex(lines[1])) && (getIndex().get(lines[1]).getStatus().equals("remove complete")))) {
                                        System.out.println("selecting Dstores");
                                        List<String> Dgo = selectDstores();
                                        String fileName = lines[1];
                                        Integer fileSize = Integer.parseInt(lines[2]);
                                        System.out.println("Adding file " + fileName + " to index");
                                        addToIndex(new FileIndex(fileName, fileSize, Dgo));
                                        System.out.println("allocating Dstores for " + fileName);
                                        allocateDstores(c, Dgo, in, out);
                                    } else {
                                        System.err.println("Error with storage request: file already exists");
                                        out.println("ERROR_FILE_ALREADY_EXISTS");
                                        out.flush();
                                        break;
                                        //c.close();
                                    }
                                } else {
                                    System.err.println("Error with storage request: not enough Dstores");
                                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    out.flush();
                                    break;
                                    //c.close();
                                }
                            }
                            String fileName = lines[1];
                            try {
                                Boolean acknow = getLatch(fileName).await(timeout, TimeUnit.MILLISECONDS);
                                if(acknow.equals(true)) {
                                    updateIndexStatus(fileName, "store complete");
                                    out.println("STORE_COMPLETE");
                                    out.flush();
                                    setLatch(fileName, R);
                                    //c.close();
                                } else {
                                    removeFromIndex(fileName);
                                    //c.close();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                removeFromIndex(fileName);
                                //c.close();
                            }
                        }
                    }
                    case "LOAD" -> {
                        setIndexToStore(0);
                        String fileName = lines[1];
                        FileIndex file;
                        if((file = getIndexFile(fileName)) != null) {
                            List<String> dstores = file.getDstoreAllocation();
                            if(!dstores.isEmpty() || file.getStatus().equals("store complete")) {
                                if(getDstoreList().size() >= R) {
                                    out.println("LOAD_FROM " + dstores.get(0) + " " + file.getFileSize().toString());
                                    out.flush();
                                    receive(c,in,out);
                                } else {
                                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    out.flush();
                                    //c.close();
                                }
                            } else {
                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                out.flush();
                                //c.close();
                            }
                        } else {
                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                            out.flush();
                        }
                    }
                    case "RELOAD" -> {
                        setIndexToStore(getIndexToStore()+1);
                        String fileName = lines[1];
                        FileIndex file = new FileIndex(getIndexFile(fileName));
                        List<String> dstores = file.getDstoreAllocation();
                        if(dstores.size() > getIndexToStore()) {
                            if(!dstores.isEmpty() || file.getStatus().equals("store complete")) {
                                if(getDstoreList().size() >= R) {
                                    out.println("LOAD_FROM " + dstores.get(getIndexToStore()) + " " + file.getFileSize().toString());
                                    out.flush();
                                    receive(c,in,out);
                                } else {
                                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    out.flush();
                                    //c.close();
                                }
                            } else {
                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                out.flush();
                                //c.close();
                            }
                        } else {
                            out.println("ERROR_LOAD");
                            out.flush();
                            //c.close();
                        }

                    }
                    case "REMOVE" -> {
                        synchronized (rebalanceLock) {
                            //this means rebalance will have to wait for a remove request to finish
                            System.out.println("client remove request: ");
                            String fileName = lines[1];
                            synchronized (Controller.class) {
                                if (getDstoreList().size() >= R) {
                                    if (isFileInIndex(lines[1])) {
                                        if (getIndexFile(fileName).getStatus().equals("store complete")) {
                                            updateIndexStatus(fileName, "remove in progress");
                                            List<String> DstoresWFile = getIndexFile(fileName).getDstoreAllocation();
                                            for(String s : DstoresWFile) {
                                                PrintWriter out1 = getDstoreI(s).getOut();
                                                out1.println("REMOVE " + fileName);
                                                out1.flush();
                                            }
                                        } else {
                                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                                            System.err.println("File requested to be removed was not a fully stored file");
                                            break;
                                        }
                                    } else {
                                        out.println("ERROR_FILE_DOES_NOT_EXIST");
                                        out.flush();
                                        break;
                                        //c.close();
                                    }
                                } else {
                                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    out.flush();
                                    break;
                                    //c.close();
                                }
                            }
                            try {
                                Boolean acknow = getLatch(fileName).await(timeout, TimeUnit.MILLISECONDS);
                                if(acknow.equals(true)) {
                                    updateIndexStatus(fileName, "remove complete");
                                    out.println("REMOVE_COMPLETE");
                                    out.flush();
                                    setLatch(fileName, R);
                                    //c.close();
                                } else {
                                    System.err.println("timeout with receiving acknowledgement of removal");
                                    //c.close();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                //c.close();
                            }
                        }
                    }
                    case "LIST" -> {
                        if(getDstoreList().size() >= R) {
                            ArrayList<FileIndex> index = new ArrayList<>(getIndexFiles());
                            if(!index.isEmpty()) {
                                out.print("LIST");
                                for (FileIndex f : index) {
                                    if(f.getStatus().equals("store complete")) {
                                        out.print(" " + f.getFileName());
                                    }
                                }
                                out.println("");
                                out.flush();
                            } else {
                                out.println("LIST ");
                                out.flush();
                            }
                        } else {
                            out.println("ERROR_NOT_ENOUGH_DSTORES");
                            out.flush();
                            //c.close();
                        }
                    }
                    default -> System.err.println("Malformed client/Dstore message received, message was: " + lines[0]);
                }
            }
            System.err.println("Receive is null, attempting to close socket for safety");
            c.close();
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
    }

    public static void rebalance() {
        synchronized (rebalanceStart) {
            synchronized (rebalanceLock) {

            }
            System.out.println("rebalance called, caling List for each Dstore");
            System.out.println("Number of Dstores: " + getDstoreList().size());
            setRebalanceCountDownLatch(getDstoreList().size());
            System.out.println("set rebalance latch to " + getDstoreList().size());
            for(DStoreI d : getDstoreList().values()) {
                d.getOut().println("LIST");
                d.getOut().flush();
            }
            System.out.println("List called, waiting for all Lists to be received");
            try {
                boolean listed = getRebalanceCountDownLatch().await(timeout, TimeUnit.MILLISECONDS);
                if(listed) {
                    System.out.println("All list replies received");
                    List<DstoreFileList> list = getRebalanceDstoreFiles();
                    sortRebalanceList(list);

                    System.out.println("Removing waste from Dstores");

                    //this should make sure files that are not in the index are removed from Dstores and files in the Index that no Dstore contains get removed from the index
                    for(DstoreFileList d : getRebalanceDstoreFiles()) {
                        for(String file : d.getFiles()) {
                            if((!isFileInIndex(file)) || ((isFileInIndex(file)) && ((!Index.get(file).getStatus().equals("store complete")) || (!Index.get(file).getDstoreAllocation().contains(d.getPort()))))) {
                                d.getFilesToRemove().add(file);
                            }
                        }
                        d.getFiles().removeAll(d.getFilesToRemove());
                    }

                    List<FileIndex> files = new ArrayList<>(getIndexFiles());

                    System.out.println("Removing waste from Index");

                    for(FileIndex f : files) {
                        List<String> actual = new ArrayList<>(f.getDstoresCorrectlyStoring());
                        List<String> ideal = new ArrayList<>(f.getDstoreAllocation());
                        //removes the file if it was not stored by any Dstore
                        if(f.getCountDownLatch().getCount() == R) {
                            getIndex().remove(f.getFileName());
                        } else if(!actual.containsAll(ideal)){
                            //this bit removes from the Index Dstores allocated to a file that do not actually contain it
                            ideal.removeAll(actual);
                            getIndexDstores(f.getFileName()).removeAll(ideal);
                        }

                        //this bit ensures that all files are replicated R times by adding them to the DstoreFileList
                        int i;
                        if((i = R - f.getDstoreAllocation().size()) > 0) {
                            for(int a = i; a > 0; a--) {
                                String name = f.getFileName();
                                DstoreFileList d = getFirstDstoreWithoutFile(name, list);
                                assert d != null;
                                String dPort = d.getPort();
                                DstoreFileList s = getFirstDstoreWithFile(name, list);
                                assert s != null;
                                List<String> locations;
                                if(s.getFilesToSend().get(name) == null) {
                                    locations = new ArrayList<>();
                                } else {
                                    locations = new ArrayList<>(s.getFilesToSend().get(name));
                                }
                                locations.add(dPort);
                                s.addFilesToSend(name, locations);
                                d.addFilesToAdd(name);
                                d.getFiles().add(name);
                                addDstoreToFileIndex(f.getFileName(), dPort);
                                sortRebalanceList(list);
                            }
                        }
                    }

                    System.out.println("making sure files are spread evenly amongst Dstores");
                    DstoreFileList first = list.get(0);
                    DstoreFileList last = list.get(list.size() - 1);
                    //this makes sure files are spread evenly amongst Dstores
                    while(first.getFiles().size() + 1 < last.getFiles().size()) {
                        String file = getFirstFileNotInSmallerDstore(last.getFiles(), first.getFiles());
                        transferFromList(last.getFiles(), last.getFilesToRemove(), file); // moving file from files to filesToRemove
                        removeDstoreFromOneFileIndex(file, last.getPort());
                        List<String> locationsToSend;
                        if(last.getFilesToSend().get(file) == null) {
                            locationsToSend = new ArrayList<>();
                        } else {
                            locationsToSend = new ArrayList<>(last.getFilesToSend().get(file));
                        }
                        locationsToSend.add(first.getPort());
                        last.addFilesToSend(file, locationsToSend);
                        // adding file to files and filesToAdd
                        first.getFilesToAdd().add(file);
                        first.getFiles().add(file);
                        addDstoreToFileIndex(file, first.getPort());
                        //sorting the rebalance list for the next iteration
                        sortRebalanceList(list);
                        first = list.get(0);
                        last = list.get(list.size() - 1);
                    }
                    System.out.println("Formulating messages to send to the Dstores");
                    System.out.println(list);
                    //now we formulate the messages and send them
                    for(DstoreFileList d : list) {
                        List<String> filesToSend = new ArrayList<>();
                        List<String> filesToRemove = new ArrayList<>();
                        filesToSend.add(Integer.toString(d.getFilesToSend().size()));
                        filesToRemove.add(Integer.toString(d.getFilesToRemove().size()));
                        filesToRemove.addAll(d.getFilesToRemove());
                        for(Map.Entry<String, List<String>> m : d.getFilesToSend().entrySet()) {
                            filesToSend.add(m.getKey());
                            filesToSend.add(Integer.toString(m.getValue().size()));
                            filesToSend.addAll(m.getValue());
                        }

                        String send = String.join(" ", filesToSend);
                        String remove = String.join(" ", filesToRemove);

                        PrintWriter out = getDstoreList().get(d.getPort()).getOut();

                        out.println("REBALANCE " + send + " " + remove);
                        out.flush();
                    }
                    System.out.println("Messages sent");
                    getRebalanceDstoreFiles().clear();
                } else {
                    System.err.println("Not all list replies received, countDownLatch at : " + getRebalanceCountDownLatch().getCount());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


        /*
        sort Dstores based on how many files they have in them, then take Dstores from the ends of the list and have them transfer a file from the biggest to the smallest,
        each transfer adds the file to the big Dstore's list to remove and adds the file to the small Dstore's list to add,this repeats until the most full and the least
        full dstore have 0-1 number of files between them.
         */
        }
    }

    public static String getFirstFileNotInSmallerDstore(List<String> bigger, List<String> smaller) {
        List<String> filesNotShared = new ArrayList<>(bigger);
        filesNotShared.removeAll(smaller);
        return filesNotShared.get(0);
    }

    public static DstoreFileList getFirstDstoreWithoutFile(String fileName, List<DstoreFileList> entry) {
        for(DstoreFileList a : entry) {
            if(!a.getFiles().contains(fileName)) {
                return a;
            }
        }
        return null;
    }

    public static DstoreFileList getFirstDstoreWithFile(String fileName, List<DstoreFileList> entry) {
        for(DstoreFileList a : entry) {
            if(a.getFiles().contains(fileName)) {
                return a;
            }
        }
        return null;
    }

    public static DstoreFileList getFirstDstoreWithFile(String name, List<DstoreFileList> entry, DstoreFileList not) {
        for(DstoreFileList a : entry) {
            if(a.getFiles().contains(name) && a != not) {
                return a;
            }
        }
        return null;
    }

    public static void transferFromList(List<String> from, List<String> to, String file) {
        from.remove(file);
        to.add(file);
    }

    public static void recieveDstoreMsg(DStoreI store) {
        try {
            String input;
            BufferedReader in = store.getIn();
            PrintWriter out = store.getOut();
            Socket c = store.getSocket();
            while((input = in.readLine()) != null) {
                String[] lines = input.split(" ");
                switch(lines[0]) {
                    case "STORE_ACK", "REMOVE_ACK" -> {
                        System.out.println("Acknowledgement received");
                        String fileName = lines[1];
                        countdownIndex(fileName);
                    }
                    case "LIST" -> {
                        System.out.println("Dstore List reply received");
                        String portName = getDStoreIPort(store);
                        if(lines.length == 1) {
                            addRebalanceDstoreFiles(new DstoreFileList(portName, new ArrayList<>()));
                        } else {
                            List<String> files = new ArrayList<>(Arrays.asList(lines).subList(1,lines.length));
                            for(String f : files) {
                                if(isFileInIndex(f)) {
                                    countdownIndex(f);
                                    getIndex().get(f).addDstoresCorrectlyStoring(portName);
                                }
                            }
                            addRebalanceDstoreFiles(new DstoreFileList(portName,files));
                        }
                        System.out.println("Decrementing rebalance countdown latch");
                        countDownRebalance();
                        System.out.println("Dstore list for " + portName + " added to the rebalance file list");
                    }
                    case "REBALANCE_COMPLETE" -> {
                        System.out.println("Rebalance complete message recieved from: " + getDStoreIPort(store));
                    }
                    case "ERROR_FILE_DOES_NOT_EXIST" -> {
                        String fileName = lines[1];
                        System.err.println("Dstore " + getDStoreIPort(store) + " Sent that the file doesn't exist for a remove request for file: " + fileName);
                    }
                    default -> System.err.println("Malformed Dstore message received, message was: " + lines[0]);
                }
            }
            System.err.println("Dstore at port " + getDStoreIPort(store) + "is returning null");
        } catch (Exception e) {
            String DstorePort = getDStoreIPort(store);
            System.err.println("Connection to Dstore: " + DstorePort + " dropped, removing Dstore from index: " + e);
            removeDStoreList(store);
            removeDstoreFromFileIndex(DstorePort);
        }

    }

    public static synchronized boolean isFileInIndex(String fileName) {
        return getIndex().containsKey(fileName);
    }

    public static void allocateDstores(Socket c, List<String> Dgo, BufferedReader in, PrintWriter out) {
        try {
            String DstoresToGo = "STORE_TO " + String.join(" ", Dgo);
            out.println(DstoresToGo);
            out.flush();
        } catch (Exception e) {
            System.err.println("Error : " + e);
        }
    }

    public static synchronized List<String> selectDstores() {
        List<String> Dstores;
        List<String> DstoresWithFiles;
        System.out.println("getting list of All dstores");
        Dstores = new ArrayList<>(getDstoreList().keySet());
        System.out.println("getting dstore locations in order of least files");
        DstoresWithFiles = getLocationsWithLeastFiles();
        Dstores.removeAll(DstoresWithFiles);
        Dstores.addAll(DstoresWithFiles);
        return Dstores.subList(0, Math.min(R,Dstores.size()));
        //DstoresWithFiles.addAll(0, Dstores);
        //return DstoresWithFiles.subList(0, Math.min(R,DstoresWithFiles.size()));
        //original fix for selecting Dstores, not as efficient as inserting all is O(n+m)
    }

    public static synchronized void sortRebalanceList(List<DstoreFileList> start) {
        start.sort(Comparator.comparingInt(obj -> obj.getFiles().size()));
    }

    public static synchronized List<String> getLocationsWithLeastFiles() {
        HashMap<String, Integer> storageCounts = new HashMap<>();
        ArrayList<FileIndex> index = getIndexFiles();
        for (FileIndex entry : index) {
            List<String> locations = entry.getDstoreAllocation();
            for (String location : locations) {
                storageCounts.put(location, storageCounts.getOrDefault(location, 0) + 1);
            }
        }
        List<String> sortedLocations = new ArrayList<>(storageCounts.keySet());
        sortedLocations.sort(Comparator.comparingInt(storageCounts::get));
        return sortedLocations;
    }
    static class controllerThread implements Runnable {

        Socket connector;
        BufferedReader in;
        PrintWriter out;


        controllerThread(Socket c, BufferedReader in, PrintWriter out) {
            this.connector = c;
            this.in = in;
            this.out = out;
        }

        public void run() {
                setIndexToStore(0);
                receive(connector, in, out);
        }
    }


    static class RebalanceThread implements  Runnable {

        RebalanceThread() {

        }

        public void run() {
            while(true) {
                synchronized (rebalanceObj) {
                    try {
                        rebalanceObj.wait(rebalancePeriod.longValue() * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                for(FileIndex f : getIndex().values()) {
                    f.setCountDownLatch(R);
                }
                rebalance();
            }
        }
    }

    public static class FileIndex {
        String fileName;
        Integer fileSize;
        List<String> DstoreAllocation;
        String status;
        CountDownLatch cdLatch;
        List<String> DstoresCorrectlyStoring;

        FileIndex(String name, Integer size, List<String> allocation) {
            fileName = name;
            fileSize = size;
            DstoreAllocation = allocation;
            status = "store in progress";
            cdLatch = new CountDownLatch(allocation.size());
            DstoresCorrectlyStoring = new ArrayList<>();
        }

        FileIndex(FileIndex f) {
            fileName = f.getFileName();
            fileSize = f.getFileSize();
            DstoreAllocation = f.getDstoreAllocation();
            status = f.getStatus();
            cdLatch = f.getCountDownLatch();
            DstoresCorrectlyStoring = f.getDstoresCorrectlyStoring();
        }

        public List<String> getDstoresCorrectlyStoring() {
            return this.DstoresCorrectlyStoring;
        }

        public void addDstoresCorrectlyStoring(String d) {
            this.DstoresCorrectlyStoring.add(d);
        }

        public void clearDstoresCorrectlyStoring() {
            this.DstoresCorrectlyStoring.clear();
        }

        public CountDownLatch getCountDownLatch() {
            return this.cdLatch;
        }

        public void setCountDownLatch(int i) {
            this.cdLatch = new CountDownLatch(i);
        }

        public void setFileName(String name) {
            this.fileName = name;
        }

        public String getFileName() {
            return this.fileName;
        }

        public void setFileSize(Integer size) {
            this.fileSize = size;
        }

        public Integer getFileSize() {
            return this.fileSize;
        }

        public void setDstoreAllocation(int index, String port) {
            this.DstoreAllocation.set(index, port);
        }

        public void setAllDstoreAllocation(List<String> allocation) {
            this.DstoreAllocation = allocation;
        }

        public void addDstoreToAllocation(String port) {
            this.DstoreAllocation.add(port);
        }

        public void removeDstoreFromAllocation(String port) {
            this.DstoreAllocation.remove(port);
        }

        public List<String> getDstoreAllocation() {
            return this.DstoreAllocation;
        }

        public String getOneDstoreAllocation(int index) {
            return this.DstoreAllocation.get(index);
        }

        public void setStatus(String st) throws Exception {
            switch (st) {
                case "store in progress", "store complete", "remove in progress", "remove complete" -> this.status = st;
                default -> throw new Exception("Wrong status");
            }
        }

        public String getStatus() {
            return this.status;
        }
    }

    public static class DStoreI {
        Socket c;
        BufferedReader in;
        PrintWriter out;


        DStoreI(Socket c, BufferedReader in, PrintWriter out) {
            this.c = c;
            this.in = in;
            this.out = out;
        }

        public synchronized Socket getSocket() {
            return this.c;
        }

        public synchronized void setSocket(Socket c) {
            this.c = c;
        }

        public synchronized BufferedReader getIn() {
            return this.in;
        }

        public synchronized void setIn(BufferedReader in) {
            this.in = in;
        }

        public synchronized PrintWriter getOut() {
            return this.out;
        }

        public synchronized void setOut(PrintWriter out) {
            this.out = out;
        }
    }

    public static class DstoreFileList {
        String port;
        List<String> files;
        List<String> filesToAdd;
        List<String> filesToRemove;
        HashMap<String, List<String>> filesToSend;

        DstoreFileList(String port, List<String> files) {
            this.port = port;
            this.files = files;
            this.filesToAdd = new ArrayList<>();
            this.filesToRemove = new ArrayList<>();
            this.filesToSend = new HashMap<>();
        }

        @Override
        public String toString() {
            return "name: " + port + " files to add: " + filesToAdd + " files to remove: " + filesToRemove + "files to send: " + filesToSend;
        }

        public synchronized HashMap<String, List<String>> getFilesToSend() {
            return this.filesToSend;
        }

        public synchronized void addFilesToSend(String name, List<String> whereToSend) {
            filesToSend.put(name, whereToSend);
        }


        public synchronized void addFilesToAdd(String file) {
            this.filesToAdd.add(file);
        }

        public synchronized List<String> getFilesToAdd() {
            return this.filesToAdd;
        }

        public synchronized void setFilesToAdd(List<String> files) {
            this.filesToAdd = files;
        }

        public synchronized void addFilesToRemove(String file) {
            this.filesToRemove.add(file);
        }

        public synchronized void setFilesToRemove(List<String> files) {
            this.filesToRemove = files;
        }

        public synchronized List<String> getFilesToRemove() {
            return this.filesToRemove;
        }

        public synchronized void setPort(String port) {
            this.port = port;
        }

        public synchronized String getPort() {
            return this.port;
        }

        public synchronized void setFiles(List<String> files) {
            this.files = files;
        }

        public synchronized List<String> getFiles() {
            return this.files;
        }
    }
}
