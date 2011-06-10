package lsr.paxos;

public interface FailureDetector {

    public interface FailureDetectorListener {
        public void suspect(int process);
    }    
    
    public void start(int initialView);
    public void stop();
    public void viewChange(int newView);
}