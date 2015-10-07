package edu.buffalo.cse562.evalRA.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.sql2RA.OpGroupBy;
import edu.buffalo.cse562.sql2RA.Operation;

public class GroupbyEvaluator extends RAEvaluator {

	public GroupbyEvaluator(DB db, Operation op,
			BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
	}
	Table tbl;
	RAEvaluator rae;
	List<Column> cl;
	Env rae_env;
	
	public void prepare() {
		OpGroupBy op = (OpGroupBy) this.op;
		cl = op.getGroupByColumnList();
		HashSet<String> group_by_set = new HashSet<String>();
		// All columns that needs group_by
		for (Column ccc:cl) {
			group_by_set.add(ccc.getColumnName());
		}
		
		GlobalConfiguration.set_group_by_set(group_by_set);
		rae = RAEvaluator.getEvaluator(this.db, op.getRight(), input);
		rae.prepare();
		
		rae_env = rae.getEnv();
		
		
		tbl = rae_env.getTable(rae.resultTable);
		this.env = this.env.mergeEnvironment(rae_env);
		this.env.group_flag = false;
		this.resultTable = op.getAlias();
		this.env.addTable(this.resultTable, tbl);
	}
	
	public void run() {
		HashMap<String, Integer> schema = tbl.getSchema();
		LinkedHashMap<String, Row> agg = new LinkedHashMap<String, Row>();
		HashMap<String, Integer> cnt_map = new HashMap<String, Integer>();
		rae.start();
		Row r = null;
		while ((r=input.poll())!=null || rae.isAlive()) {
			if (r == null)
				continue;
			
			String val = "";
			for (Column c:cl) {
				String col = c.getColumnName();
				int nc = schema.get(col);
				val += ((Record)r.getRecord(nc)).toString()+"|";
			}
			val = val.substring(0, val.length()-1);
		
			if (cnt_map.get(val) == null) {
				cnt_map.put(val, 1);
			} else {
				cnt_map.put(val, cnt_map.get(val)+1);
			}
			
			Row c = agg.get(val);
			if (c == null) {
				agg.put(val, r.clone());
			} else {
				// TODO: Aggregate
				// Summation, for count and sum
				for (String key: schema.keySet()) {
					String agg_op = rae_env.column_agg_op.get(key);
					if (agg_op != null) {
						int nc = schema.get(key);
						Record rcd = (Record)c.getRecord(nc);
						
						Record op_rcd = (Record)r.getRecord(nc);
						rcd.add(op_rcd);
					}
				}
			}
		}
		
		while ((r=input.poll())!=null) {
			String val = "";
			for (Column c:cl) {
				String col = c.getColumnName();
				int nc = schema.get(col);
				val += ((Record)r.getRecord(nc)).toString()+"|";
			}
			val = val.substring(0, val.length()-1);
		
			if (cnt_map.get(val) == null) {
				cnt_map.put(val, 1);
			} else {
				cnt_map.put(val, cnt_map.get(val)+1);
			}
			
			Row c = agg.get(val);
			if (c == null) {
				agg.put(val, r.clone());
			} else {
				// TODO: Aggregate
				// Summation, for count and sum
				for (String key: schema.keySet()) {
					String agg_op = rae_env.column_agg_op.get(key);
					if (agg_op != null) {
						int nc = schema.get(key);
						Record rcd = (Record)c.getRecord(nc);
						
						Record op_rcd = (Record)r.getRecord(nc);
						rcd.add(op_rcd);
					}
				}
			}
		}
		
		for (String k:agg.keySet()) {
			Row cur = agg.get(k);
			for (String key: schema.keySet()) {
				String agg_op = rae_env.column_agg_op.get(key);
				if (agg_op != null && agg_op.compareTo("avg") == 0) {
					int nc = schema.get(key);
					Record rcd = (Record)cur.getRecord(nc);
					Integer cnt = cnt_map.get(k);
					rcd.div(cnt);
				}
			}
			output.add(agg.get(k));
		}
		
		Thread.currentThread().interrupt();
		if (GlobalConfiguration.debug){ 
			System.out.println("Finished group_by");
		}
	}
}
