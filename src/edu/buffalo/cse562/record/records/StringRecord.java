package edu.buffalo.cse562.record.records;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import jdbm.SerializerOutput;
import edu.buffalo.cse562.record.Record;

public class StringRecord extends Record {
	private static final long serialVersionUID = 1L;
	
	public String val;
	
	public StringRecord(String val) {
		this.val = val;
	}
	
	public int compareTo(String v) {
		return val.compareTo(v);
	}

	@Override
	public int compareTo(Double val) {
		try {
			throw new Exception("Incompatible type between string and "
					+ "double in compareTo");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return 0;
	}

	@Override
	public int compareTo(Integer val) {
		try {
			throw new Exception("Incompatible type between string and "
					+ "integer in compareTo");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return 0;
	}

	@Override
	public int compareTo(Long val) {
		try {
			throw new Exception("Incompatible type between string and "
					+ "long in compareTo");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return 0;
	}

	@Override
	public String toString() {
		return this.val;
	}

	@Override
	public void add(Double val) {
		try {
			throw new Exception("Incompatible type between string and "
					+ "double in add");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

	@Override
	public void div(Integer val) {
		try {
			throw new Exception("Incompatible type between string and "
					+ "integer in div");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public int compareTo(Record r) {
		StringRecord rcd = (StringRecord) r;
		return this.val.toUpperCase().compareTo(rcd.val.toUpperCase());
	}

	@Override
	public void add(Record val) {
		throw new IllegalArgumentException("Unsupported addition in string!");
	}

	@Override
	public void writeValue(SerializerOutput out) {
		try {
			out.writeUTF(this.val);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}		
	}
	
	public Long longValue() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		Long result = null;
		try {
			result = dateFormat.parse(this.val).getTime();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return result; 
	}
}
