package lsr.paxos;

public interface FailureDetector {

    public interface FailureDetectorListener {
        /**
         * The failure detector suspected the leader of the current view
         * 
         * @param view The view whose leader was suspected.
         */
        public void suspect(int view);
    }

    public void start(int initialView);

    public void stop();

    /**
     * Inform the failure detector that the process advanced to a new view.
     * 
     * The failure detector will also advance to this view. If the local process
     * is the leader of the view, the FD will start sending heartbeats.
     * Otherwise, it starts waiting for heartbeats from the leader of newView.
     * 
     * @param newView
     */
    public void viewChange(int newView);
}