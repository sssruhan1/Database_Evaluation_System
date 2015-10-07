package edu.buffalo.cse562.record;

import java.io.Serializable;

import jdbm.SerializerOutput;

abstract public class Record implements Serializable{
	private static final long serialVersionUID = 1L;
	public abstract int compareTo(Double val);
	public abstract int compareTo(String val);
	public abstract int compareTo(Integer val);
	public abstract int compareTo(Long val);
	public abstract int compareTo(Record r);
	public abstract String toString();
	public abstract void add(Double val);
	public abstract void add(Record val);
	public abstract void div(Integer val);
	public abstract void writeValue(SerializerOutput out);
	public abstract Long longValue();
	
	public void error(String err) {
		try {
			throw new Exception(err);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}