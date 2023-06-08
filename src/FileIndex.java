import java.util.List;

public class FileIndex {
    String fileName;
    Integer fileSize;
    List<String> DstoreAllocation;
    String status;

    FileIndex(String name, Integer size, List<String> allocation) {
        fileName = name;
        fileSize = size;
        DstoreAllocation = allocation;
        status = "store in progress";
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
        switch(st) {
            case "store in progress":
            case "store complete":
            case "remove in progress":
            case "remove complete":
                this.status = st;
                break;
            default:
                throw new Exception("Wrong status");
        }
    }

    public String getStatus() {
        return this.status;
    }
}
