/*
 * #!
 * %
 * Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */
package application.bolts.lr.util;



/**
 * {@link AvgValue} is an class that helps to compute an average.
 * 
 * @author mjsax
 */
public final class AvgValue {
	/** The current sum over all values. */
	private double sum;
	
	/** The current number of base values. */
	private int count;
	
	
	
	/**
	 * Instantiates a new {@link AvgValue} object with initial value.
	 * 
	 * @param initalValue
	 *            the first value of the average
	 */
	public AvgValue(double initalValue) {
		this.sum = initalValue;
		this.count = 1;
	}
	
	
	
	/**
	 * Adds a new value to the average.
	 * 
	 * @param value
	 *            the value to be added to the average
	 */
	public void updateAverage(double value) {
		this.sum += value;
		++this.count;
	}
	
	/**
	 * Returns the current average.
	 * 
	 * @return the current average
	 */
	public Double getAverage() {
		return new Double(this.sum / this.count);
	}
	
}
