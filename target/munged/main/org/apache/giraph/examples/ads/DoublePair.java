package org.apache.giraph.examples.ads;

/**
 * A pair of doubles.
 */
public class DoublePair {
  /** First element. */
  private double first;
  /** Second element. */
  private double second;

  /** Constructor.
   *
   * @param fst First element
   * @param snd Second element
   */
  public DoublePair(double fst, double snd) {
    first = fst;
    second = snd;
  }

  /**
   * Get the first element.
   *
   * @return The first element
   */
  public double getFirst() {
    return first;
  }

  /**
   * Set the first element.
   *
   * @param first The first element
   */
  public void setFirst(double first) {
    this.first = first;
  }

  /**
   * Get the second element.
   *
   * @return The second element
   */
  public double getSecond() {
    return second;
  }

  /**
   * Set the second element.
   *
   * @param second The second element
   */
  public void setSecond(double second) {
    this.second = second;
  }
}