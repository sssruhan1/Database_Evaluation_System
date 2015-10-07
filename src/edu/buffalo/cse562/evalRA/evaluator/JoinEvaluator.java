package edu.buffalo.cse562.evalRA.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.record.records.LongRecord;
import edu.buffalo.cse562.sql2RA.OpJoin;
import edu.buffalo.cse562.sql2RA.Operation;

public class JoinEvaluator extends RAEvaluator {

	public JoinEvaluator(DB db, Operation op, BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
		this.opj = (OpJoin) op;
	}

	private RAEvaluator rae_l;
	private RAEvaluator rae_r;
	private HashMap<String, Integer> schema_l;
	private HashMap<String, Integer> schema_r;
	private String left_join_key;
	private BoundedConcurrentLinkedQueue input2 = new BoundedConcurrentLinkedQueue();
	private String join_key;
	private OpJoin opj = null;
	public void prepare() {
		rae_l = RAEvaluator.getEvaluator(db, opj.getLeft(), input);
		rae_r = RAEvaluator.getEvaluator(db, opj.getRight(), input2);
		rae_l.prepare();
		rae_r.prepare();
		 
		this.resultTable = genTableName();
		Env rae_l_env = rae_l.getEnv();
		Env rae_r_env = rae_r.getEnv();
		ArrayList<String> al = new ArrayList<String>();
			
		Table tl = rae_l_env.getTable(rae_l.resultTable);
		Table tr = rae_r_env.getTable(rae_r.resultTable);
			
		
		schema_l = tl.getSchema();
		schema_r = tr.getSchema();
			
		EqualsTo eqt = (EqualsTo)op.getExpression();
		Column lc = (Column) eqt.getLeftExpression();
			
		join_key = lc.getColumnName();
		left_join_key = rae_l_env.aliasLookUp(lc.getTable().toString(), join_key);
		if (left_join_key == null) {
			left_join_key = join_key;
		}
					
		if (GlobalConfiguration.debug){ 
			System.out.println(rae_l.resultTable + ": " + left_join_key + " " + rae_r.resultTable + ": " +  join_key);
		}
		
		HashMap<String, String> type_l= tl.getType();
		HashMap<String, String> type_r= tr.getType();
			
		for (String k: schema_l.keySet()) {
			al.add(k);
		}
		for (String k: schema_r.keySet()) {
			if (schema_l.get(k) != null) {
				String new_name = genJoinName() + k;
				this.env.addTableAlias(rae_r.resultTable, k, new_name);
				k = new_name;
			}
			al.add(k);
		}
	
			
		Table t = new Table(al);
			
		this.env.addTable(resultTable, t);
		this.env = this.env.mergeEnvironment(rae_l.env);
		this.env = this.env.mergeEnvironment(rae_r.env);
		for (String k: type_l.keySet()) {
			t.setType(k, type_l.get(k));
		}
		for (String k: type_r.keySet()) {
			t.setType(k, type_r.get(k));
		}
			
		
	}
	
	public void run() {
		rae_r.start();
		rae_l.start();
		
		
		HashMap<Long, LinkedList<Row>> index = new HashMap<Long, LinkedList<Row>>();
		
		Row rr=null;

		while ((rr=(Row)input2.poll())!=null ||
			   rae_r.isAlive()) {
			if (rr == null)
				continue;

			int nc = schema_r.get(join_key);
			LongRecord rcd = (LongRecord)rr.getRecord(nc);
			
			if (GlobalConfiguration.needs_swap) {
			} else {
				LinkedList<Row> rows = index.get(rcd.val);
				if (rows != null) {				
					rows.add(rr);
				} else {
					LinkedList<Row> arr = new LinkedList<Row>();
					arr.add(rr);
					index.put(rcd.val, arr);
				}
				//buffer1.add(rr);
			}
		}
		
		while ((rr=(Row)input2.poll())!=null) {
			int nc = schema_r.get(join_key);
			LongRecord rcd = (LongRecord)rr.getRecord(nc);
			
			if (GlobalConfiguration.needs_swap) {
			} else {
				LinkedList<Row> rows = index.get(rcd.val);
				if (rows != null) {
					rows.add(rr);
				} else {
					LinkedList<Row> arr = new LinkedList<Row>();
					arr.add(rr);
					index.put(rcd.val, arr);
				}
				//buffer1.add(rr);
			}
		}
		
		Row rl = null;
	
		
		while ((rl=(Row) input.poll())!=null|| rae_l.isAlive()) {
			if (rl == null) {
				continue;
			}
			int nc = schema_l.get(left_join_key);
			LongRecord rcd = (LongRecord)rl.getRecord(nc);
			String rcd_k = rcd.toString();
			if (GlobalConfiguration.needs_swap) {
			} else {
				LinkedList<Row> rows = index.get(rcd.val);
				if (rows != null) {
					for (Row tmp_r:rows) {
						//Row tmp_r = buffer1.get(ni);
						Row ro = rl.clone();
						ro.append(tmp_r);
						output.add(ro);
					}
				}
			}
		}
		
		while ((rl=(Row) input.poll())!=null) {
			int nc = schema_l.get(left_join_key);
			LongRecord rcd = (LongRecord)rl.getRecord(nc);
			String rcd_k = rcd.toString();
			if (GlobalConfiguration.needs_swap) {
			} else {
				LinkedList<Row> rows = index.get(rcd.val);
				if (rows != null) {
					for (Row tmp_r:rows) {
						//Row tmp_r = buffer1.get(ni);
						Row ro = rl.clone();
						ro.append(tmp_r);
						output.add(ro);
					}
				}
			}
		}
		
		if (GlobalConfiguration.debug) {
			System.out.println("Done Join--");
		}
		
		Thread.currentThread().interrupt();
	}
}
