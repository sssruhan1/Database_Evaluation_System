package edu.buffalo.cse562.evalRA.evaluator;

import java.util.ArrayList;
import java.util.HashMap;

import jdbm.PrimaryTreeMap;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.IndexBucket;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.record.records.LongRecord;
import edu.buffalo.cse562.sql2RA.IndexJoin;
import edu.buffalo.cse562.sql2RA.OpTable;
import edu.buffalo.cse562.sql2RA.Operation;

public class IndexJoinEvaluator extends RAEvaluator {

	public IndexJoinEvaluator(DB db, Operation op,
				BoundedConcurrentLinkedQueue output) {
			super(db, op, output);
			// TODO Auto-generated constructor stub
			this.opj = (IndexJoin) op;
		}
	
		private RAEvaluator rae_l;
		private RAEvaluator rae_r;
		private HashMap<String, Integer> schema_l;
		private HashMap<String, Integer> schema_r;
		private String left_join_key;
		private BoundedConcurrentLinkedQueue input2 = new BoundedConcurrentLinkedQueue();
		private String join_key;
		private IndexJoin opj = null;
		
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
				
			if (GlobalConfiguration.debug) {
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
			// Assume indexes are always on right hand side.
			if (opj.getLeft() instanceof OpTable) {
				String tblName = rae_l.resultTable;
				Table leftTable = rae_l.env.getTable(tblName);
				PrimaryTreeMap<Long, IndexBucket> leftIndex = leftTable.getIndex(opj.indexName1);
				rae_r.start();
				Row rr = null;
				
				while ((rr=(Row) input2.poll())!=null || rae_r.isAlive()) {
					if (rr == null) {
						continue;
					}
					int nc = schema_r.get(join_key);
					LongRecord rcd = (LongRecord)rr.getRecord(nc);
					IndexBucket bucket = leftIndex.get(rcd.val);
					if (bucket != null) { 			
						for (Long mr:bucket.getRows()) {
							Row tmp_l = leftTable.indexGetRow(mr);
							Row ro = tmp_l.clone();
							ro.append(rr);
							output.add(ro);
						}
					}
				}
				
				while ((rr=(Row) input2.poll())!=null) {
					int nc = schema_r.get(join_key);
					LongRecord rcd = (LongRecord)rr.getRecord(nc);
					IndexBucket bucket = leftIndex.get(rcd.val);
					if (bucket != null) { 			
						for (Long mr:bucket.getRows()) {
							Row tmp_l = leftTable.indexGetRow(mr);
							Row ro = tmp_l.clone();
							ro.append(rr);
							output.add(ro);
						}
					}
				}
				
				if (GlobalConfiguration.debug) {
					System.out.println("Done Join Right --");
				}
				
				Thread.currentThread().interrupt();
				return;
			}
			
			String tblName = rae_r.resultTable;
			Table rightTable = rae_r.env.getTable(tblName);
			
			if (GlobalConfiguration.debug) {
				System.out.println("Index loop join on: " + tblName + " " + join_key);
			}
			
			PrimaryTreeMap<Long, IndexBucket> rightIndex = rightTable.getIndex(join_key);   
			rae_l.start();
			
			
			Row rl = null;
			
			while ((rl=(Row) input.poll())!=null|| rae_l.isAlive()) {
				if (rl == null) {
					continue;
				}
				int nc = schema_l.get(left_join_key);
				LongRecord rcd = (LongRecord)rl.getRecord(nc);
				IndexBucket bucket = rightIndex.get(rcd.val);
				if (bucket != null) { 			
					for (Long mr:bucket.getRows()) {
						Row tmp_r = rightTable.indexGetRow(mr);
						Row ro = rl.clone();
						ro.append(tmp_r);
						output.add(ro);
					}
				}
	
			}
			
			while ((rl=(Row) input.poll())!=null) {
				int nc = schema_l.get(left_join_key);
				LongRecord rcd = (LongRecord)rl.getRecord(nc);
				IndexBucket bucket = rightIndex.get(rcd.val);
				if (bucket != null) { 		
					for (Long mr:bucket.getRows()) {
						Row tmp_r = rightTable.indexGetRow(mr);
						Row ro = rl.clone();
						ro.append(tmp_r);
						output.add(ro);
					}
				}
			}
			
			
			if (GlobalConfiguration.debug) {
				System.out.println("Done Join--");
			}
			
			Thread.currentThread().interrupt();
		}

}
