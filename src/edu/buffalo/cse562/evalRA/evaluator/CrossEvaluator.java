package edu.buffalo.cse562.evalRA.evaluator;

import java.util.ArrayList;
import java.util.HashMap;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.sql2RA.OpCross;
import edu.buffalo.cse562.sql2RA.Operation;

public class CrossEvaluator extends RAEvaluator {

	public CrossEvaluator(DB db, Operation op, BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
	}
	
	public void run() {
		BoundedConcurrentLinkedQueue input2 = new BoundedConcurrentLinkedQueue();
		OpCross op = (OpCross) this.op;
		RAEvaluator rae_l = RAEvaluator.getEvaluator(db, op.getLeft(), input);
		RAEvaluator rae_r = RAEvaluator.getEvaluator(db, op.getRight(), input2);
	
		rae_l.start();
		rae_r.start();
		
		
		this.resultTable = genTableName();
		Env rae_l_env = rae_l.getEnv();
		Env rae_r_env = rae_r.getEnv();
		ArrayList<String> al = new ArrayList<String>();
		
		Table tl = rae_l_env.getTable(rae_l.resultTable);
		Table tr = rae_r_env.getTable(rae_r.resultTable);
		
		HashMap<String, Integer> schema_l = tl.getSchema();
		HashMap<String, Integer> schema_r = tr.getSchema();
		
		String join_key = "";
		for (String k1:schema_l.keySet()) {
			for (String k2:schema_r.keySet()) {
				if (k1.compareTo(k2) == 0 && k1.compareTo("comment")!=0) { 
					join_key = k1;
				}
			}
		}
		
		HashMap<String, ArrayList<Integer>> index = new HashMap<String, ArrayList<Integer>>();
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
		
		ArrayList<Row> buffer1 = new ArrayList<Row>();
		
		Row rl=null;
		int i = 0;
		while ((rl=input.poll())!=null ||
			   rae_l.isAlive()) {
			if (rl == null)
				continue;
			int nc = schema_l.get(join_key);
			Record rcd = (Record)rl.getRecord(nc);
			
			if (index.get(rcd.toString())!=null) {
				ArrayList<Integer> rows = index.get(rcd.toString());
				rows.add(i);
			} else {
				ArrayList<Integer> arr = new ArrayList<Integer>();
				arr.add(i);
				index.put(rcd.toString(), arr);
			}
			buffer1.add(rl);
			i++;
		}
		
		Row rr = null;
		while ((rr=(Row) input2.poll())!=null|| rae_r.isAlive()) {
			if (rr == null) {
				continue;
			}
			int nc = schema_r.get(join_key);
			Record rcd = (Record)rr.getRecord(nc);
			if (index.get(rcd.toString())!=null) {
				ArrayList<Integer> rows = index.get(rcd.toString());
				for (Integer ni:rows) {
					Row tmp_r = buffer1.get(ni);
					Row ro = tmp_r.clone();
					ro.append(rr);
					//					System.out.println(ro.row.size());
					output.add(ro);
				}
			}
		}
	}
}
