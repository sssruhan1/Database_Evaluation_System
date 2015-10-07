package edu.buffalo.cse562.evalRA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.evalRA.evaluator.Env;
import edu.buffalo.cse562.evalRA.evaluator.RAEvaluator;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.record.records.DoubleRecord;
import edu.buffalo.cse562.record.records.LongRecord;
import edu.buffalo.cse562.record.records.StringRecord;
import edu.buffalo.cse562.sql2RA.OpSelectCondition;
import edu.buffalo.cse562.sql2RA.OpTable;
import edu.buffalo.cse562.sql2RA.OpTarget;
import edu.buffalo.cse562.sql2RA.Operation;

public class Evaluator {
	private DB db;
	
	public Evaluator(DB db) {
		this.db = db;
	}
	
	public void eval(Operation op) {
		BoundedConcurrentLinkedQueue buffer = new BoundedConcurrentLinkedQueue();
		Row r = null;
		RAEvaluator rae = RAEvaluator.getEvaluator(db, op, buffer);
		rae.prepare();
		Env env = rae.getEnv();
		
		long start = System.currentTimeMillis();

		Record my_agg = new LongRecord(new Long(0));
		rae.run();
		while ((r=buffer.poll())!=null || rae.isAlive()) {
			if (r == null)
				continue;
			if (env.group_flag) {
				// TODO This needs improvement
				Record t = r.getRecord(0);
				my_agg.add(t);
			} else { 
				System.out.println(r);
			}
		}
		
		while ((r=buffer.poll())!=null) {
			if (env.group_flag) {
				// TODO This needs improvement
				Record t = (Record)r.getRecord(0);
				my_agg.add(t);
			} else { 
				System.out.println(r);
			}
		}
		
		long end = System.currentTimeMillis();
		
		if (GlobalConfiguration.debug) {
			System.out.println("Takes: " + (end - start) / 1000 + " secs");
		}

		if (GlobalConfiguration.debug){
		}
		// TODO This needs further improvement
		if (env.group_flag){
			System.out.println(my_agg);
		}
		
	}
	
	public ArrayList<Row> evalSelection(Operation op) {
		ArrayList<Row> result = new ArrayList<Row>();
		BoundedConcurrentLinkedQueue buffer = new BoundedConcurrentLinkedQueue();
		Row r = null;
		RAEvaluator rae = RAEvaluator.getEvaluator(db, op, buffer);
		rae.prepare();
		
		long start = System.currentTimeMillis();

		rae.run();
		while ((r=buffer.poll())!=null || rae.isAlive()) {
			if (r == null)
				continue;
			result.add(r);
		}
		
		while ((r=buffer.poll())!=null) {
			result.add(r);
		}
		
		long end = System.currentTimeMillis();
		
		if (GlobalConfiguration.debug) {
			System.out.println("Eval Selection Takes: " + (end - start) / 1000 + " secs");
		}

		return result;
	}
	
	public Table evaluateOp(Operation op, BoundedConcurrentLinkedQueue buffer, Stack<Thread> thrd) {
		BoundedConcurrentLinkedQueue q = new BoundedConcurrentLinkedQueue();
		
		if (op instanceof OpTable) {
			OpTable o = (OpTable) op;
			Table tbl = db.getTable(o.getAlias());
			Thread t = tbl.AsyncRead(buffer); 
			thrd.push(t);
			
			return db.getTable(o.getAlias());
		}
		
		if (op instanceof OpTarget) {
			OpTarget o = (OpTarget) op;
			
			Table t = evaluateOp(o.getLeft(), q, thrd);
			HashMap<String, Integer> schema = t.getSchema();
			List<SelectExpressionItem> items = o.getTargetList();
			
			Object obj = null;
			
			while ((thrd.peek().isAlive()) || (obj = q.poll())!=null) {
				if (obj == null)
					continue;
				Row r = (Row)obj;
				Row nr = new Row();
				
				for (SelectExpressionItem itm:items) {
					Expression e = itm.getExpression();
					if (e instanceof Column) {
						Column c = (Column) e;
						String sc = c.getColumnName();
						int nc = schema.get(sc);
						nr.addRecord(r.getRecord(nc));
					}
				}
				buffer.add(nr);
			}
			
			ArrayList<String> nsms = new ArrayList<String>();
			for (SelectExpressionItem itm:items) {
				nsms.add(expressionName(itm.getExpression()));
			}
			Table rt = new Table(nsms);
			return rt;
		}
		
		if (op instanceof OpSelectCondition) {
			OpSelectCondition osc = (OpSelectCondition) op;
			
			Table t = evaluateOp(op.getLeft(), q, thrd);
			HashMap<String, Integer> schema = t.getSchema();
			
			Expression e = osc.getExpression();
			Object obj = null;
			while (thrd.peek().isAlive() || (obj=q.poll()) != null) {
				Row r = (Row) obj;
				
				if (r == null) {
					continue;
				}
				
				Boolean b = (Boolean)evalExpression(e, r, schema);
				
				if (b.booleanValue()) {	
					buffer.add(r);
				}
			}
			return t;
		}
		
		return null;
	}
	
	public Object evalExpression(Expression e, Row row, HashMap<String, Integer> schema) {
		if (e instanceof AndExpression) {
			AndExpression ea = (AndExpression) e;
			Expression l = ea.getLeftExpression();
			Expression r = ea.getRightExpression();
			Boolean rl = (Boolean) evalExpression(l, row, schema);
			if (!rl.booleanValue()) {
				return rl;
			}
		
			Boolean rr = (Boolean) evalExpression(r, row, schema);

			return rr;
		}
		
		if (e instanceof GreaterThanEquals) {
			GreaterThanEquals gte = (GreaterThanEquals) e;
			Expression l = gte.getLeftExpression();
			Expression r = gte.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, schema);
			Record rr = (Record) evalExpression(r, row, schema);
			
			return (rl.compareTo(rr)>=0);
		}
		
		if (e instanceof MinorThan) {
			MinorThan gte = (MinorThan) e;
			Expression l = gte.getLeftExpression();
			Expression r = gte.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, schema);
			Record rr = (Record) evalExpression(r, row, schema);
			

			return (rl.compareTo(rr)<0);
		}
		
		if (e instanceof GreaterThan) {
			GreaterThan gte = (GreaterThan) e;
			Expression l = gte.getLeftExpression();
			Expression r = gte.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, schema);
			Record rr = (Record) evalExpression(r, row, schema);
			
			return (rl.compareTo(rr)>0);
		}
		
		if (e instanceof Column) {
			String c = ((Column) e).getColumnName();
			int nc = schema.get(c);
			return row.getRecord(nc);
		}
		
		if (e instanceof LongValue) {
			return new LongRecord(((LongValue)e).getValue());
		}
		
		if (e instanceof StringValue) {
			return new StringRecord(((StringValue)e).getValue());
		}
		
		if (e instanceof DoubleValue) {
			return new DoubleRecord(((DoubleValue)e).getValue());
		}
		
		if (e instanceof Function) {
			Function fe = (Function) e;
			switch(fe.getName().toLowerCase()) {
			case "date":
				String ds = fe.getParameters().getExpressions().get(0).toString();
				return new StringRecord(ds.substring(1, ds.length()-1));
			}
		}
		
		System.out.println("Unsupported expression: " + e.toString());
		
		return null;		
	}
	
	public String expressionName(Expression e) {
		if (e instanceof Column) {
			return ((Column) e).getColumnName();
		}
		
		return genName();
	}
	
	int c = 0;
	public String genName() {
		return "__Symb__" + (c++);
	}
}
