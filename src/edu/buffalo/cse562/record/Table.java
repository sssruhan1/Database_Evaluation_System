package edu.buffalo.cse562.record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.records.DoubleRecord;
import edu.buffalo.cse562.record.records.LongRecord;
import edu.buffalo.cse562.record.records.StringRecord;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class Table {	
	public LinkedHashMap<String, Integer> schema = new LinkedHashMap<String, Integer>();
	public LinkedHashMap<String, String> schema_type = new LinkedHashMap<String, String>();
	public ArrayList<Row> rows = new ArrayList<Row>();
	public PrimaryStoreMap<Long, Row> table_storage = null;
	public String filename;
	public List<ColumnDefinition> fields;
	public BufferedReader br;
	
	public Table(HashMap<String, String> schema_type_defs) {
		int i = 0;
		for (String k: schema_type_defs.keySet()) {
			schema.put(k, i++);
			schema_type.put(k,  schema_type_defs.get(k));
		}
	}
	
	public Table(ArrayList<String> columns) {
		for (int i=0; i<columns.size(); i++) {
			schema.put(columns.get(i), i);
		}
	}
	public Table(String filename, List<ColumnDefinition> fields) {
		int i=0;
		for (ColumnDefinition cd: fields) {
			schema.put(cd.getColumnName(), i);
			schema_type.put(cd.getColumnName(), cd.getColDataType().getDataType());
			i++;
		}
		this.filename = filename;
		this.fields = fields;
		try {
			
			// Hard coded index field to prevent file loading!
			if (!GlobalConfiguration.build_index) {
				return;
			}
			
			long start = System.currentTimeMillis();
			
			this.br = new BufferedReader(new FileReader(filename));
			Row r = readRow();
			while (r != null) {
				this.rows.add(r);
				r = readRow();
			}
			br.close();
			long end = System.currentTimeMillis();
			
			if (GlobalConfiguration.debug){
				System.out.println("Read on: " + filename + " takes: " + ((end - start) / 1000) + " secs");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private final int LONG = 0;
	private final int DOUBLE = 1;
	private final int STRING = 2;
	
	public Row readRow() {
		Row row = new Row();
		ArrayList<Integer> types = null;
		
		try {
			int i;	
			String line = br.readLine();
			if (line == null) {
				br.close();
				return null;
			}
			
			String[] items = line.split("\\|");
			i = 0;
			
			if (types == null) {
				types = new ArrayList<Integer>();
				for (ColumnDefinition cd:fields) {
					String t = cd.getColDataType().getDataType();
					types.add(fieldTypeInt(t));
				}
			}
			
			for (int tp:types) {
				row.addRecord(castData(tp, items[i]));
				i++;
			}
			return row;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return row;
	}
	
	public int fieldTypeInt(String t) {
		switch (t.toLowerCase()) {
		case "int":
		case "integer":
			return LONG;
		case "date":
		case "string":
		case "varchar":
		case "char":
			return STRING;
		case "decimal":
			return DOUBLE;
		}
		
		return -1;
	}
	
	public Record castData(int t, String data) throws Exception {
		switch (t) {
		case LONG:
			return new LongRecord(Long.parseLong(data));
		case STRING:
			if (data.equals("1-urgent")) {
				System.out.println("ERROR!");
				System.exit(1);
			}
			return new StringRecord(data);
		case DOUBLE:
			return new DoubleRecord(Double.parseDouble(data));
		default:
			throw new Exception("Unknown data type: " + t);
		}
	}
	
	public Row getRow(int i) {
		return rows.get(i);
	}
	
	public String colType(String col) {
		return schema_type.get(col);
	}
	
	public void setType(String col, String t) {
		this.schema_type.put(col,  t);
	}
	
	public LinkedHashMap<String, Integer> getSchema() {
		return schema;
	}
	
	public LinkedHashMap<String, String> getType() {
		return schema_type;
	}

	public ReadThread AsyncRead(BoundedConcurrentLinkedQueue buffer) {
		ReadThread t = new ReadThread(buffer, this);
		return t;
	}
		
	public Table clone() {
		Table t = new Table(this.filename, this.fields);
		t.fields = this.fields;
		t.filename = this.filename;
		t.schema = this.schema;
		t.schema_type = this.schema_type;
		t.table_storage = this.table_storage;
		t.indexes = this.indexes;
		return t;
	}
	
	public ArrayList<Row> getRows() {
		return this.rows;
	}
	
	public void close() {
	}
	
	private HashMap<String, PrimaryTreeMap<Long, IndexBucket>> indexes = 
			new HashMap<String, PrimaryTreeMap<Long, IndexBucket>>();
	
	public PrimaryTreeMap<Long, IndexBucket> getIndex(String key) {
		return indexes.get(key);
	}
	
	public Row indexGetRow(long r) {
		return table_storage.get(r);
	}
	
	public PrimaryStoreMap<Long, Row> getStorage() {
		return this.table_storage;
	}
	
	public String tableName = "null_Table";
	public ArrayList<Integer> primary_fields = null;
	
	public String getPrimaryKey(Row row) throws Exception {
		if (primary_fields == null) {
			primary_fields = new ArrayList<Integer>();
			switch(tableName) {
			case "lineitem":
				primary_fields.add(schema.get("orderkey"));
				primary_fields.add(schema.get("linenumber"));
				break;
			case "orders":
				primary_fields.add(schema.get("orderkey"));
				break;
			case "part":
				primary_fields.add(schema.get("partkey"));
				break;
			case "customer":
				primary_fields.add(schema.get("custkey"));
				break;
			case "supplier":
				primary_fields.add(schema.get("suppkey"));
				break;
			case "partsupp":
				primary_fields.add(schema.get("partkey"));
				primary_fields.add(schema.get("suppkey"));
				break;
			case "nation":
				primary_fields.add(schema.get("nationkey"));
				break;
			case "region":
				primary_fields.add(schema.get("regionkey"));
				break;
			default:
				throw new Exception("Unsupported table: " + tableName);
			}
		}
		
		String result = "";
		for (int nc:primary_fields) {
			result += row.getRecord(nc);
		}
		
		return result;
	}
	
	public PrimaryTreeMap<String, Long> primaryKeyInverse = null;
	
	public void createIndex(RecordManager recman, String tblName) {
		tableName = tblName;
		if (GlobalConfiguration.debug) {
			System.out.println("Indexing table: " + tblName);
		}
		
		table_storage = recman.storeMap(tblName, new RowSerializer(schema_type));
		primaryKeyInverse = recman.treeMap("primary_inverse" + tblName);
		
		for (String key: getIndexKeys(tblName)) {
			if (GlobalConfiguration.debug && GlobalConfiguration.build_index) {
				System.out.println("Creating index on key: " + key + " for table " + tblName);
			}
			PrimaryTreeMap<Long, IndexBucket> treeMap = 
					recman.treeMap(tblName + "_" + key, new IndexBucketSerializer());
			indexes.put(key, treeMap);
		}
		
		table_storage.addRecordListener(new PrimaryRecordListener(indexes, schema));
		
		if (!GlobalConfiguration.build_index) return;
		
		long i = 0;
		
		for (Row r:rows) {
			long v = table_storage.putValue(r);
			
			try {
				primaryKeyInverse.put(getPrimaryKey(r), v);
			} catch (Exception e1) {
				e1.printStackTrace();
				System.exit(1);
			}
			
			i++;
			if (i % 2000 == 0) {
				try {
					recman.commit();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
	}
	
	public ArrayList<String> getIndexKeys(String table_name) {
		ArrayList<String> keys = new ArrayList<String>();
		switch (table_name) {
		case "lineitem":
			keys.add("shipdate");
			keys.add("orderkey");
			//keys.add("linenumber");
			break;
		case "orders":
			keys.add("orderdate");
			keys.add("orderkey");
			keys.add("custkey");
			break;
		case "part":
			keys.add("partkey");
			break;
		case "customer":
			keys.add("custkey");
			break;
		case "supplier":
			keys.add("suppkey");
			break;
		case "partsupp":
			keys.add("partkey");
			break;
		}
		
		return keys;
	}
}
