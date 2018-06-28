/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twosigma.flint.math;

import java.io.Serializable;

/**
 * This uses the {@linktourl http://en.wikipedia.org/wiki/Kahan_summation_algorithm}
 * to minimize errors when adding a large amount of doubles.
 *
 * This implements a slightly modification of Kahan Summation named Neumaier Summation.
 *
 * Note it is mutable and not thread safe.
 */
public class Kahan implements Serializable {

    // The current running sum
    private double runningSum;

    // The current running compensation
    private double runningCompensation;

    /**
     * Construct an instance with specific initial value
     *
     * @param initialValue specifies the initial value.
     */
    public Kahan(double initialValue) {
        this();
        runningSum = initialValue;
    }

    /**
     * Construct an instance with 0.0 initial value
     */
    public Kahan() {
        runningSum = 0.0;
        runningCompensation = 0.0;
    }

    /**
     * Add a value to this Kahan summation.
     *
     * @param value specifies a value to add.
     */
    public void add(double value) {
        double currentRunningSum = runningSum + value;
        if (Math.abs(runningSum) >= Math.abs(value)) {
            runningCompensation += (runningSum - currentRunningSum) + value;
        } else {
            runningCompensation += (value - currentRunningSum) + runningSum;
        }
        runningSum = currentRunningSum;
    }

    /**
     * Combine another Kahan summation to this Kahan summation.
     *
     * @param value specifies another Kahan summation to add.
     */
    public void add(Kahan value) {
        add(value.runningSum);
        add(value.runningCompensation);
    }

    /**
     * Subtract another Kahan summation from this Kahan summation.
     *
     * @param value specifies another Kahan summation to subtract.
     */
    public void subtract(Kahan value) {
        add(-value.runningSum);
        add(-value.runningCompensation);
    }

    /**
     * @return the resulting value of this Kahan summation.
     */
    public double value() {
        return runningSum + runningCompensation;
    }

    @Override
    public String toString() {
        return "Kahan {" +
                "runningSum=" + runningSum +
                ", runningCompensation=" + runningCompensation +
                '}';
    }
}
