package edu.buffalo.cse562.evalRA.evaluator;

import java.util.ArrayList;
import java.util.HashSet;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.record.records.LongRecord;
import edu.buffalo.cse562.sql2RA.OpAggregate;
import edu.buffalo.cse562.sql2RA.Operation;

public class AggregateEvaluator extends RAEvaluator {

	public AggregateEvaluator(DB db, Operation op,
			BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
	}
	Expression e;
	RAEvaluator rae;
	Env rae_env;
	String col_name;
	Table tbl;
	
	public void prepare() {
		OpAggregate op = (OpAggregate) this.op;
		e = op.getExpression();
		
		rae = RAEvaluator.getEvaluator(this.db, op.getRight(), input);
		rae.prepare();
		rae_env = rae.getEnv();
		tbl = rae_env.getTable(rae.resultTable);
		
		this.resultTable = op.getAlias();
		col_name = op.getAlias();
		ArrayList<String> al = new ArrayList<String>();
		
		for (String k: tbl.getSchema().keySet()) {
			al.add(k);
		}
		al.add(col_name);	
		
		Table t = new Table(al);
		for (String k: tbl.getSchema().keySet()) {
			t.setType(k, tbl.colType(k));			
		}
		
		// TODO TYPE
		
		this.env.addTable(this.resultTable, t);
		this.env = this.env.mergeEnvironment(rae_env);
		this.env.group_flag = true;
		
		if (e instanceof Function) {
			Function fe = (Function) e;
			this.env.column_agg_op.put(col_name, fe.getName().toLowerCase());
			
			if (fe.getParameters() != null) {
				col = (Expression) (fe.getParameters().getExpressions().get(0));
			}
		}
	}

	Expression col = null; 
	
	public void run() {
		rae.start();
		
		// TODO: Table Type Info
		if (e instanceof Function) {
			Function fe = (Function) e;
			// TODO: Function support
			// These are all mappers <- we calculate it as a map-reduce manner!
			

			switch (fe.getName().toLowerCase()) {
			case "count":		
				HashSet<String> distinct_set = new HashSet<String>();
				Row rt;
				while ((rt=input.poll())!=null || rae.isAlive()) {
					if (rt == null)
						continue;
					Row tt = rt.clone();
					if (fe.isDistinct()) {
						Record rcd = (Record) this.evalExpression(col, tt, tbl, this.env);
						String dis_str = rcd.toString();
						for (String cgp:GlobalConfiguration.group_by_set) {
							int nc = tbl.getSchema().get(cgp);
							Record rcd_grp = (Record) tt.getRecord(nc);
							dis_str += rcd_grp.toString();
						}
						if (distinct_set.contains(dis_str)) {
							tt.addRecord(new LongRecord((long) 0));
						} else {
							tt.addRecord(new LongRecord((long) 1));
							distinct_set.add(dis_str);
						}
					} else {					
						tt.addRecord(new LongRecord((long) 1));
					}
					output.add(tt);
				}
				
				while ((rt=input.poll())!=null) {
					Row tt = rt.clone();
					if (fe.isDistinct()) {
						Record rcd = (Record) this.evalExpression(col, tt, tbl, this.env);
						String dis_str = rcd.toString();
						for (String cgp:GlobalConfiguration.group_by_set) {
							int nc = tbl.getSchema().get(cgp);
							Record rcd_grp = (Record) tt.getRecord(nc);
							dis_str += rcd_grp.toString();
						}
						if (distinct_set.contains(dis_str)) {
							tt.addRecord(new LongRecord((long) 0));
						} else {
							tt.addRecord(new LongRecord((long) 1));
							distinct_set.add(dis_str);
						}
					} else {					
						tt.addRecord(new LongRecord((long) 1));
					}
					output.add(tt);
				}
				break;
			case "sum":
			case "avg":
				while ((rt=input.poll())!=null || rae.isAlive()) {
					if (rt == null)
						continue;
					Row tt = rt.clone();
					Record rcd = (Record) this.evalExpression(col, tt, tbl, this.env);
					tt.addRecord(rcd);
					output.add(tt);
//					System.out.println(tt.row.size());
				}
				while ((rt=input.poll())!=null) {
					Row tt = rt.clone();
					Record rcd = (Record) this.evalExpression(col, tt, tbl, this.env);
					tt.addRecord(rcd);
					output.add(tt);
				}
				break;
			default:
			}
		} else {
			Row r = null;
			Row rm;
			while((r = input.poll())!=null || rae.isAlive()) {
				if (r == null)
					continue;
				Record result = (Record) evalExpression(e, r, tbl, rae_env);
				rm = r.clone();
				rm.addRecord(result);
				output.add(rm);
			}
			while((r = input.poll())!=null) {
				Record result = (Record) evalExpression(e, r, tbl, rae_env);
				rm = r.clone();
				rm.addRecord(result);
				output.add(rm);
			}
		}
		
		Thread.currentThread().interrupt();
	}
}
