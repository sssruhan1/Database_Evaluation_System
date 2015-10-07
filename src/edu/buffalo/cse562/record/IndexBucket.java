package edu.buffalo.cse562.record;

import java.util.HashSet;

public class IndexBucket {
	private int n_rows;
	private HashSet<Long> rows = null;

	public IndexBucket() {
		n_rows = 0;
		rows = new HashSet<Long>();
	}
	
	public void putRow(long n) {
		n_rows++;
		rows.add(n);
	}
	
	public int getSize() {
		return this.n_rows;
	}
	
	public HashSet<Long> getRows() {
		return this.rows;
	}
	
	public void delRow(Long n) {
		n_rows--;
		rows.remove(n);
	}
}
