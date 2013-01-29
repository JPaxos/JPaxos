package lsr.paxos;

public interface FailureDetector {

    public interface FailureDetectorListener {
        /**
         * The failure detector suspected the leader of the current view
         * 
         * The FD will call this method ONCE a view.
         * 
         * @param view The view whose leader was suspected.
         */
        public void suspect(int view);
    }

    public void start(int initialView);

    public void stop();
}