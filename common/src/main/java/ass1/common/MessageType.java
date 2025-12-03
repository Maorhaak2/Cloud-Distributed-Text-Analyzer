package ass1.common;

public enum MessageType {

    NEW_TASK,           
    ANALYZE,        
    WORKER_DONE,    
    SUMMARY_DONE,
    WORKER_ERROR;
    


    @Override
    public String toString() {
        return name();
    }
}