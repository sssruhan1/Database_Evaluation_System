package edu.buffalo.cse562.evalRA.evaluator;

import java.util.HashMap;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.record.records.DoubleRecord;
import edu.buffalo.cse562.record.records.LongRecord;
import edu.buffalo.cse562.record.records.StringRecord;
import edu.buffalo.cse562.sql2RA.IndexFilterJoin;
import edu.buffalo.cse562.sql2RA.IndexJoin;
import edu.buffalo.cse562.sql2RA.IndexNestedLoopJoin;
import edu.buffalo.cse562.sql2RA.IndexRangeScan;
import edu.buffalo.cse562.sql2RA.IndexScan;
import edu.buffalo.cse562.sql2RA.OpAggregate;
import edu.buffalo.cse562.sql2RA.OpCross;
import edu.buffalo.cse562.sql2RA.OpGroupBy;
import edu.buffalo.cse562.sql2RA.OpJoin;
import edu.buffalo.cse562.sql2RA.OpLimit;
import edu.buffalo.cse562.sql2RA.OpOrderBy;
import edu.buffalo.cse562.sql2RA.OpSelectCondition;
import edu.buffalo.cse562.sql2RA.OpTable;
import edu.buffalo.cse562.sql2RA.OpTarget;
import edu.buffalo.cse562.sql2RA.Operation;

public abstract class RAEvaluator extends Thread {
	protected DB db = null;
	protected BoundedConcurrentLinkedQueue input = new BoundedConcurrentLinkedQueue();
	protected BoundedConcurrentLinkedQueue output = null;
	protected Operation op;
	protected Thread t = null;
	protected Env env = new Env();
	protected String resultTable = null;
	protected static int c = 0; 
	protected boolean isfinished = false;

	public RAEvaluator(DB db, Operation op, BoundedConcurrentLinkedQueue output) {
		this.db = db;
		this.op = op;
		this.output = output;
		this.env.addTables(this.db.getTables());
	}
	
	public void prepare() {
		
	}
	
	public Env getEnv() {
		return this.env;
	}
	
	public String getTableName() {
		return resultTable;
	}
	
	public String expressionName(Expression e) {
		if (e instanceof Column) {
			Column c = (Column)e;
			String col_name = this.env.aliasLookUp(c.getTable().getName(), c.getColumnName());
			
			return col_name;
		}
		return "__EXPR__" + (c++);
	}
	
	public String genTableName() {
		return "__Table__" + (c++);
	}
	
	public String genJoinName() {
		return "__JOIN__" + (c++);
	}
	
	public static RAEvaluator getEvaluator(DB db, Operation op, BoundedConcurrentLinkedQueue output) {
		if (op instanceof OpTable) {
			return new TableEvaluator(db, op, output);
		}
		
		if (op instanceof OpTarget) {
			return new TargetEvaluator(db, op, output);
		}
		
		if (op instanceof IndexRangeScan) {
			return new IndexRangeScanEvaluator(db, op, output);
		}
		
		if (op instanceof IndexScan) {
			return new IndexScanEvaluator(db, op, output);
		}
		
		if (op instanceof OpSelectCondition) {
			return new SelectionConditionEvaluator(db, op, output);
		}
		
		if (op instanceof OpAggregate) {
			return new AggregateEvaluator(db, op, output);
		}
		
		if (op instanceof OpGroupBy) {
			return new GroupbyEvaluator(db, op, output);
		}
		
		if (op instanceof OpOrderBy) {
			return new OrderbyEvaluator(db, op, output);
		}
		
		if (op instanceof OpCross) {
			return new CrossEvaluator(db, op, output);
		}
		
		if (op instanceof OpLimit) {
			return new LimitEvaluator(db, op, output);
		}
		
		if (op instanceof IndexFilterJoin) {
			return new IndexFilterJoinEvaluator(db, op, output);
		}
		if (op instanceof IndexJoin) {
			return new IndexJoinEvaluator(db, op, output);
		}
		/*
		if (op instanceof IndexNestedLoopJoin) {
			return new IndexNestedJoinEvaluator(db, op, output);
		}*/
		
		if (op instanceof OpJoin) {
			return new JoinEvaluator(db, op, output);
		}
		
		if (op == null) {
			System.out.println("Null op!");
		}
		
		System.out.println("Unsupported evaluator " + op.toString());
		
		return null;
	}
	
	public Object evalExpression(Expression e, Row row, Table t, Env env) {
		HashMap <String, Integer> schema = t.getSchema();

		if (e instanceof DateValue) {
			return new StringRecord(((DateValue)e).toString());
		}
		
		if (e instanceof LongValue) {
			return new LongRecord(((LongValue)e).getValue());
		}
		
		if (e instanceof StringValue) {
			String value = ((StringValue) e).getValue();
			return new StringRecord(value);
		}
		
		if (e instanceof DoubleValue) {
			return new DoubleRecord(((DoubleValue)e).getValue());
		}
		
		if (e instanceof AndExpression) {
			AndExpression ea = (AndExpression) e;
			Expression l = ea.getLeftExpression();
			Expression r = ea.getRightExpression();
			
			
			Boolean rl = (Boolean) evalExpression(r, row, t, env);
			if (!rl.booleanValue()) {
				return rl;
			}
		
			Boolean rr = (Boolean) evalExpression(l, row, t, env);

			return rr;
		}
		
		if (e instanceof OrExpression) {
			OrExpression ea = (OrExpression) e;
			Expression l = ea.getLeftExpression();
			Expression r = ea.getRightExpression();
			
			Boolean rl = (Boolean) evalExpression(l, row, t, env);
			if (rl.booleanValue()) {
				return rl;
			}
		
			Boolean rr = (Boolean) evalExpression(r, row, t, env);

			return rr;
		}
		
		if (e instanceof GreaterThanEquals) {
			GreaterThanEquals gte = (GreaterThanEquals) e;
			Expression l = gte.getLeftExpression();
			Expression r = gte.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);

			return (rl.compareTo(rr)>=0);
		}
		
		if (e instanceof EqualsTo) {
			EqualsTo gte = (EqualsTo) e;
			Expression l = gte.getLeftExpression();
			Expression r = gte.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);
			
			return (rl.compareTo(rr)==0);
		}
		
		if (e instanceof NotEqualsTo) {
			NotEqualsTo net = (NotEqualsTo) e;
			Expression l = net.getLeftExpression();
			Expression r = net.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);

			return (rl.compareTo(rr)!=0);
		}
		
		if (e instanceof MinorThan) {
			MinorThan gte = (MinorThan) e;
			Expression l = gte.getLeftExpression();
			Expression r = gte.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);
			

			return (rl.compareTo(rr)<0);
		}
		
		if (e instanceof MinorThanEquals) {
			MinorThanEquals gte = (MinorThanEquals) e;
			Expression l = gte.getLeftExpression();
			Expression r = gte.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);
			

			return (rl.compareTo(rr)<=0);
		}
		
		if (e instanceof GreaterThan) {
			GreaterThan gte = (GreaterThan) e;
			Expression l = gte.getLeftExpression();
			Expression r = gte.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);
			

			return (rl.compareTo(rr)>0);
		}
		
		if (e instanceof Addition) {
			Addition sub = (Addition) e;
			Expression l = sub.getLeftExpression();
			Expression r = sub.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);
			
			// TODO: Date
			if (rl instanceof LongRecord && rr instanceof LongRecord) {
				LongRecord ll = (LongRecord) rl;
				LongRecord lr = (LongRecord) rr;
				return new LongRecord(ll.val + lr.val);
			}
			
			if (rl instanceof DoubleRecord && rr instanceof DoubleRecord) {
				DoubleRecord a = (DoubleRecord)rl;
				DoubleRecord b = (DoubleRecord)rr;
				return new DoubleRecord(a.val + b.val);
			}
			
			if (rl instanceof LongRecord && rr instanceof DoubleRecord) {
				LongRecord a = (LongRecord) rl;
				DoubleRecord b = (DoubleRecord) rr;
				return new DoubleRecord(a.val + b.val);
			}
			
			if (rl instanceof DoubleRecord && rr instanceof LongRecord) {
				LongRecord b = (LongRecord)rr;
				DoubleRecord a = (DoubleRecord)rl;
				return new DoubleRecord(a.val + b.val);
			}
			
			// Date
			//String a = (String)rl;
			// TODO Date:
		}
		
		if (e instanceof Subtraction) {
			Subtraction sub = (Subtraction) e;
			Expression l = sub.getLeftExpression();
			Expression r = sub.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);
			
			if (rl instanceof LongRecord && rr instanceof LongRecord) {
				LongRecord ll = (LongRecord) rl;
				LongRecord lr = (LongRecord) rr;
				return new LongRecord(ll.val - lr.val);
			}
			
			if (rl instanceof DoubleRecord && rr instanceof DoubleRecord) {
				DoubleRecord a = (DoubleRecord)rl;
				DoubleRecord b = (DoubleRecord)rr;
				return new DoubleRecord(a.val - b.val);
			}
			
			if (rl instanceof LongRecord && rr instanceof DoubleRecord) {
				LongRecord a = (LongRecord) rl;
				DoubleRecord b = (DoubleRecord) rr;
				return new DoubleRecord(a.val - b.val);
			}
			
			if (rl instanceof DoubleRecord && rr instanceof LongRecord) {
				LongRecord b = (LongRecord)rr;
				DoubleRecord a = (DoubleRecord)rl;
				return new DoubleRecord(a.val - b.val);
			}
			// Date
			//String a = (String)rl.record;
			// TODO Date:
		}
		
		if (e instanceof Multiplication) {
			Multiplication sub = (Multiplication) e;
			Expression l = sub.getLeftExpression();
			Expression r = sub.getRightExpression();
			
			Record rl = (Record) evalExpression(l, row, t, env);
			Record rr = (Record) evalExpression(r, row, t, env);
			
			if (rl instanceof LongRecord && rr instanceof LongRecord) {
				LongRecord ll = (LongRecord) rl;
				LongRecord lr = (LongRecord) rr;
				return new LongRecord(ll.val * lr.val);
			}
			
			if (rl instanceof DoubleRecord && rr instanceof DoubleRecord) {
				DoubleRecord a = (DoubleRecord)rl;
				DoubleRecord b = (DoubleRecord)rr;
				return new DoubleRecord(a.val * b.val);
			}
			
			if (rl instanceof LongRecord && rr instanceof DoubleRecord) {
				LongRecord a = (LongRecord) rl;
				DoubleRecord b = (DoubleRecord) rr;
				return new DoubleRecord(a.val * b.val);
			}
			
			if (rl instanceof DoubleRecord && rr instanceof LongRecord) {
				LongRecord b = (LongRecord)rr;
				DoubleRecord a = (DoubleRecord)rl;
				return new DoubleRecord(a.val * b.val);
			}
			
		}
		
		if (e instanceof Parenthesis) {
			Parenthesis p = (Parenthesis) e;
			Record rl = (Record) evalExpression(p.getExpression(), row, t, env);
			return rl;
		}
		
		if (e instanceof Column) {
			String c = expressionName(e);
			int nc = schema.get(c);
			return row.getRecord(nc);
		}
		
		if (e instanceof Function) {
			Function fe = (Function) e;
			switch(fe.getName().toLowerCase()) {
			case "date":
				String ds = fe.getParameters().getExpressions().get(0).toString();
				return new StringRecord(ds.substring(1, ds.length()-1));
			}
			
		}
		
		return null;		
	}
	
	synchronized boolean isFinished() {
		return this.isfinished;
	}
	
	synchronized void finish() {
		this.isfinished = true;
	}
}
