package edu.buffalo.cse562.record;

import java.util.concurrent.ConcurrentLinkedQueue;

public class BoundedConcurrentLinkedQueue extends ConcurrentLinkedQueue<Row> {

	private static final long serialVersionUID = 1L;

	public BoundedConcurrentLinkedQueue() {
		super();
	}
	
	public Row poll() {
		return super.poll();
	}
	
	public boolean add(Row o) {
		super.add(o);
		return true;
	}
}
