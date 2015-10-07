package edu.buffalo.cse562.record.records;

import java.io.IOException;

import jdbm.SerializerOutput;
import edu.buffalo.cse562.record.Record;

public class DoubleRecord extends Record {
	private static final long serialVersionUID = 1L;
	public Double val;
	
	public DoubleRecord(double f) {
		val = f;
	}

	@Override
	public int compareTo(Double val) {
		return this.val.compareTo(val);
	}

	@Override
	public int compareTo(String val) {
		error("incompatible type between double and string in compareTo");
		return 0;
	}

	@Override
	public int compareTo(Integer val) {
		return this.val.compareTo(val.doubleValue());
	}

	@Override
	public int compareTo(Long val) {
		return this.val.compareTo(val.doubleValue());
	}

	@Override
	public String toString() {
		return this.val.toString();
	}

	@Override
	public void add(Double val) {
		this.val = this.val + val;
	}

	@Override
	public void div(Integer val) {
		this.val = this.val / val;
	}

	@Override
	public int compareTo(Record r) {
		DoubleRecord rcd =(DoubleRecord) r;
		return this.val.compareTo(rcd.val);
	}

	@Override
	public void add(Record val) {
		DoubleRecord rcd = (DoubleRecord) val;
		this.val = this.val + rcd.val;
	}

	@Override
	public void writeValue(SerializerOutput out) {
		try {
			out.writeDouble(this.val);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
	public Long longValue() {
		try {
			throw new Exception("Unimplemented longValue for Double!");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return new Long(0);
	}
}
