package edu.buffalo.cse562.record.records;

import java.io.IOException;

import jdbm.SerializerOutput;
import edu.buffalo.cse562.record.Record;

public class LongRecord extends Record {
	private static final long serialVersionUID = 1L;
	
	public Long val;
	public LongRecord(Long val) {
		this.val = val;
	}
	
	@Override
	public int compareTo(Double val) {
		return -val.compareTo(this.val.doubleValue());
	}

	@Override
	public int compareTo(String val) {
		error("Incompatible type between Long and String in comapreTo");
		return 0;
	}

	@Override
	public int compareTo(Integer val) {
		return this.val.compareTo(val.longValue());
	}

	@Override
	public int compareTo(Long val) {
		return this.val.compareTo(val);
	}

	@Override
	public String toString() {
		return this.val.toString();
	}

	@Override
	public void add(Double val) {
		this.val = this.val + val.longValue();
	}

	@Override
	public void div(Integer val) {
		this.val = this.val / val;
	}

	@Override
	public int compareTo(Record r) {
		LongRecord rcd = (LongRecord) r;
		return this.val.compareTo(rcd.val);
	}

	@Override
	public void add(Record val) {
		LongRecord rcd = (LongRecord)val;
		this.val = rcd.val + this.val;
	}

	@Override
	public void writeValue(SerializerOutput out) {
		try {
			out.writeLong(this.val);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
	public Long longValue() {
		return this.val;
	}
}
