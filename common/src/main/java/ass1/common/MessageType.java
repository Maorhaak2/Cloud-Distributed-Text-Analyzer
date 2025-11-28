package ass1.common;

public enum MessageType {

    NEW_TASK,       
    TERMINATE,      
    ANALYZE,        
    WORKER_DONE,    
    SUMMARY_DONE;

    @Override
    public String toString() {
        return name();
    }
}