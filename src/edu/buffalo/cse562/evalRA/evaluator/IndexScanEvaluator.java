package edu.buffalo.cse562.evalRA.evaluator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import jdbm.PrimaryTreeMap;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.IndexBucket;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.record.records.StringRecord;
import edu.buffalo.cse562.sql2RA.IndexScan;
import edu.buffalo.cse562.sql2RA.OpSelectCondition;
import edu.buffalo.cse562.sql2RA.Operation;

public class IndexScanEvaluator extends RAEvaluator {
	public IndexScanEvaluator(DB db, Operation op,
			BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
		// TODO Auto-generated constructor stub
	}

	RAEvaluator rae;
	Env rae_env;
	Table tbl;
	Expression exp;
	public void prepare() {
		OpSelectCondition sc = (OpSelectCondition) op;
		rae = RAEvaluator.getEvaluator(this.db, op.getRight(), this.input);
		rae.prepare();
		rae_env = rae.getEnv();
		exp = sc.getExpression();
		this.resultTable = genTableName();		
		tbl = rae_env.getTable(rae.resultTable);
		this.env.addTable(this.resultTable, tbl);
		this.env = this.env.mergeEnvironment(rae.env);
	}
	
	public void run() {
		if (exp instanceof EqualsTo) {
			IndexScan is = (IndexScan) op;
			String item = is.indexName;
			Expression right = ((EqualsTo) exp).getRightExpression();
			StringRecord indexVal = (StringRecord) evalExpression(right, null, tbl, env);
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			try {
				Date date = dateFormat.parse(indexVal.val);
				long time = date.getTime();
				PrimaryTreeMap<Long, IndexBucket> index = tbl.getIndex(item);
				IndexBucket bucket = index.get(time);
				if (bucket != null) {
					for (long key:bucket.getRows()) {
						Row r = tbl.table_storage.get(key);
						output.add(r);
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
				System.exit(1);
			}
			return;
		}
		
		rae.start();
		Row r = null;
		
		while((r=input.poll())!=null || rae.isAlive()) {
			if (r == null)
				continue;
			
			Boolean b = (Boolean)evalExpression(exp, r, tbl, env);
			
			if (b.booleanValue()) {	
				output.add(r);
			}	
		}
		
		while((r=input.poll())!=null) {
			Boolean b = (Boolean)evalExpression(exp, r, tbl, env);
		
			if (b.booleanValue()) {	
				output.add(r);
			}
		}
		
		Thread.currentThread().interrupt();
	}
}
