package edu.buffalo.cse562.record.records;

import java.io.IOException;

import jdbm.SerializerOutput;
import edu.buffalo.cse562.record.Record;

public class IntRecord extends Record {
	private static final long serialVersionUID = 1L;
	
	public Integer val;
	
	public IntRecord(Integer val) {
		this.val = val;
	}
	
	@Override
	public int compareTo(Double val) {		
		return -val.compareTo(this.val.doubleValue());
	}

	@Override
	public int compareTo(String val) {
		error("Incompatible type between integer and string in compareTo");
		return 0;
	}

	@Override
	public int compareTo(Integer val) {
		return this.val.compareTo(val);
	}

	@Override
	public int compareTo(Long val) {
		return -val.compareTo(this.val.longValue());
	}

	@Override
	public String toString() {
		return this.val.toString();
	}

	@Override
	public void add(Double val) {
		this.val = val.intValue() + this.val;
	}

	@Override
	public void div(Integer val) {
		this.val = this.val / val;
	}

	@Override
	public int compareTo(Record r) {
		IntRecord rcd = (IntRecord)r;
		return this.val.compareTo(rcd.val);
	}

	@Override
	public void add(Record val) {
		IntRecord rcd = (IntRecord)val;
		this.val = rcd.val + this.val;
	}

	@Override
	public void writeValue(SerializerOutput out) {
		try {
			out.writeInt(this.val);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
	public Long longValue() {
		return this.val.longValue();
	}
}
