package lsr.common;

/**
 * Represents exponential moving average
 * 
 * A moving average is the simplest average, to calculate it we need only last
 * value and a constant how much from the new value has to be taken.
 * 
 * The recursion follows as this: factor*newValue + (1-factor)*oldValue
 * 
 * Transforming from recursion to typical equation it turns out that the weights
 * for older elements are exponentially fading. If, after some time the input
 * stabilizes to a constant, the bigger our factor is, the faster our average
 * approaches this constant. That's why we call it a "convergence" factor.
 */
public class MovingAverage {
    /** How important is the last value - range (0,1) */
    private final double convergenceFactor;

    /** Starting point if no given */
    private double average = 0;

    public MovingAverage(double convergenceFactor) {
        if (0 >= convergenceFactor || convergenceFactor >= 1) {
            throw new IllegalArgumentException("Incorrect convergence factor in moving average.");
        }
        this.convergenceFactor = convergenceFactor;
    }

    public MovingAverage(double convergenceFactor, double firstAverage) {
        this(convergenceFactor);
        average = firstAverage;
    }

    /** Calculates next average basing on next value */
    public double add(double value) {
        average = (1 - convergenceFactor) * average + convergenceFactor * value;
        return average;
    }

    /**
     * (re)Starts the calculation with newAverage, that is treats newAverage as
     * the current value
     */
    public void reset(double newAverage) {
        average = newAverage;
    }

    /** Returns the current value */
    public double get() {
        return average;
    }
}
