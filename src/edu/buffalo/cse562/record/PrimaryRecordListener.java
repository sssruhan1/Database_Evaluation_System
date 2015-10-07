package edu.buffalo.cse562.record;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import edu.buffalo.cse562.configurations.GlobalConfiguration;
import jdbm.PrimaryTreeMap;
import jdbm.RecordListener;

public class PrimaryRecordListener implements RecordListener<Long, Row> {
	private HashMap<String, PrimaryTreeMap<Long, IndexBucket>> indexes = null;
	private LinkedHashMap<String, Integer> schema = null;
	
	public PrimaryRecordListener(HashMap<String, PrimaryTreeMap<Long, IndexBucket>> _indexes,
								 LinkedHashMap<String, Integer> _schema){ 
		this.indexes = _indexes;
		this.schema = _schema;
	}
	@Override
	public void recordInserted(Long k, Row row) throws IOException {
		for (String key: indexes.keySet()) {
			PrimaryTreeMap<Long, IndexBucket> treeMap = indexes.get(key);					
			int nc = schema.get(key);
					
			Record rcd = row.getRecord(nc);
			IndexBucket bucket = treeMap.get(rcd.longValue());
			if (bucket == null) {
				bucket = new IndexBucket();
				bucket.putRow(k);
				treeMap.put(rcd.longValue(), bucket);
			} else {
				bucket.putRow(k);
				treeMap.put(rcd.longValue(), bucket);
			}
		}	
	}

	@Override
	public void recordRemoved(Long k, Row row) throws IOException {
		for (String key: indexes.keySet()) {
			PrimaryTreeMap<Long, IndexBucket> treeMap = indexes.get(key);					
			int nc = schema.get(key);
					
			Record rcd = row.getRecord(nc);
			IndexBucket bucket = treeMap.get(rcd.longValue());
			if (bucket != null) {
				bucket.delRow(k);
				treeMap.put(rcd.longValue(), bucket);
			}
		}
	}

	@Override
	public void recordUpdated(Long k, Row row_old, Row row_new) throws IOException {
		for (String key: GlobalConfiguration.update_index_keys) {
			PrimaryTreeMap<Long, IndexBucket> treeMap = indexes.get(key);					
			int nc = schema.get(key);
					
			Record rcd = row_old.getRecord(nc);
			IndexBucket bucket = treeMap.get(rcd.longValue());
			if (bucket != null) {
				bucket.delRow(k);
				treeMap.put(rcd.longValue(), bucket);
			}
		}
		
		for (String key: GlobalConfiguration.update_index_keys) {
			PrimaryTreeMap<Long, IndexBucket> treeMap = indexes.get(key);					
			int nc = schema.get(key);
					
			Record rcd = row_new.getRecord(nc);
			IndexBucket bucket = treeMap.get(rcd.longValue());
			if (bucket == null) {
				bucket = new IndexBucket();
				bucket.putRow(k);
				treeMap.put(rcd.longValue(), bucket);
			} else {
				bucket.putRow(k);
				treeMap.put(rcd.longValue(), bucket);
			}
		}	
		//recordRemoved(k, row_old);
		//recordInserted(k, row_new);
	}

}
