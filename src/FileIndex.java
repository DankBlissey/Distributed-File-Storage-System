
public class FileIndex {
    String fileName;
    Integer fileSize;
    Integer[] DstoreAllocation;
    String status;

    FileIndex(String name, Integer size, Integer[] allocation) {
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

    public void setDstoreAllocation(int index, Integer port) {
        this.DstoreAllocation[index] = port;
    }

    public Integer[] getDstoreAllocation() {
        return this.DstoreAllocation;
    }

    public Integer getOneDstoreAllocation(int index) {
        return this.DstoreAllocation[index];
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
