package edu.buffalo.cse562.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import edu.buffalo.cse562.record.records.DoubleRecord;
import edu.buffalo.cse562.record.records.LongRecord;
import edu.buffalo.cse562.record.records.StringRecord;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class RowSerializer implements Serializer<Row> {
	private ArrayList<Integer> schemas = new ArrayList<Integer>();
	public final int DOUBLE = 0;
	public final int STRING = 1;
	public final int LONG = 2;	

	public RowSerializer(LinkedHashMap<String, String> schema) {
		for (String k:schema.keySet()) {
			try {
				schemas.add(getType(schema.get(k)));
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public int getType(String t) throws Exception{
		switch (t.toLowerCase()) {
		case "int":
		case "integer":
			return LONG;
		case "string":
		case "date":
		case "varchar":
			return STRING;
		case "decimal":
			return DOUBLE;
		case "char":
			return STRING;
		default:
			throw new Exception("Unknown data type: " + t);
		}
	}
	
	public Record createRecord(int type, SerializerInput in) throws Exception {
		Record rcd;
		
		switch (type) {
		case LONG:			
			rcd = new LongRecord(in.readLong());
			break;
		case STRING:
			rcd = new StringRecord(in.readUTF());
			break;
		case DOUBLE:
			rcd = new DoubleRecord(in.readDouble());
			break;
		default:
			throw new Exception("Unsupported type " + type);
		}
		
		return rcd;
	}
	
	@Override
	public Row deserialize(SerializerInput in) throws IOException,
			ClassNotFoundException {
		Row row = new Row();
		try {
			for (int s:schemas) {
				Record rcd = createRecord(s, in);
				row.addRecord(rcd);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return row;
	}

	@Override
	public void serialize(SerializerOutput out, Row row) throws IOException {	
		for (Record rcd : row.row) {
			rcd.writeValue(out);
		}
	}
}
