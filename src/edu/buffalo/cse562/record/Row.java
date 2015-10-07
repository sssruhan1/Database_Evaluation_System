package edu.buffalo.cse562.record;

import java.io.Serializable;
import java.util.ArrayList;

public class Row implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public ArrayList<Record> row = new ArrayList<Record>();
	
	public void addRecord(Record r) {
		row.add(r);
	}
	
	public Record getRecord(int index) {
		return row.get(index);
	}
	
	public Row clone() {
		Row r = new Row();
		for (Record re : row) {
			r.addRecord(re);
		}
		
		return r;
	}
	
	public void append(Row r) {
		for (Record i: r.row) {
			this.addRecord(i);
		}
	}
	
	@Override
	public String toString() {
		String s = "";
		for (Record r:row) {
			s += r.toString() + "|";
		}
		return s.substring(0, s.length()-1);
	}
}