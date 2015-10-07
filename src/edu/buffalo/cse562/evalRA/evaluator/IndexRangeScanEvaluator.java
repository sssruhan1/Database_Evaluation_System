package edu.buffalo.cse562.evalRA.evaluator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

import jdbm.PrimaryTreeMap;
import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.record.BoundedConcurrentLinkedQueue;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.IndexBucket;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.sql2RA.IndexRangeScan;
import edu.buffalo.cse562.sql2RA.Operation;

public class IndexRangeScanEvaluator extends RAEvaluator {

	public IndexRangeScanEvaluator(DB db, Operation op,
			BoundedConcurrentLinkedQueue output) {
		super(db, op, output);
		
	}
	
	RAEvaluator rae;
	Env rae_env;
	Table tbl;
	Expression exp;
	Long rangeLow, rangeHigh;
	
	public void prepare() {
		IndexRangeScan sc = (IndexRangeScan) op;
		rae = RAEvaluator.getEvaluator(this.db, op.getRight(), this.input);
		rae.prepare();
		rae_env = rae.getEnv();
		exp = sc.getExpression();
		this.resultTable = genTableName();		
		tbl = rae_env.getTable(rae.resultTable);
		this.env.addTable(this.resultTable, tbl);
		this.env = this.env.mergeEnvironment(rae.env);
		
		if (GlobalConfiguration.debug) 
			System.out.println("Range low is: " + sc.rangeLow + " " + sc.rangeHigh);
		
		if (sc.rangeLow.contains("date")) {
			rangeLow = adjustLow(sc);
			rangeHigh = adjustHigh(sc);
		} else {
			rangeLow = Long.parseLong(sc.rangeLow);
			rangeHigh = Long.parseLong(sc.rangeHigh);
			if (!sc.lowInclusive) {
				rangeLow += 1;
			}
			
			if (sc.highInclusive){
				rangeHigh += 1;
			}
		}
	}
		
	public void run() {
		IndexRangeScan sc = (IndexRangeScan) op;
		String tblName = sc.getTableName();
		String key = sc.indexName;
		
		Table table = env.getTable(tblName);
		
		if (GlobalConfiguration.debug) {
			System.out.println("Scan on " + tblName + " " + key);
		}
		
		PrimaryTreeMap<Long, IndexBucket> treeMap = table.getIndex(key);
		
		Collection<IndexBucket> buckets = treeMap.subMap(rangeLow, rangeHigh).values();
		
		for (IndexBucket bucket:buckets) {
			for (Long mr:bucket.getRows()) {
				Row r = table.indexGetRow(mr);
				output.add(r);
			}
		}		
		Thread.currentThread().interrupt();
	}

	Date getDate(String s) {
		int start = s.indexOf("'") + 1;
		int end = s.indexOf("'", start);
		String tmp = s.substring(start, end);
		
		Date date = null;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			date = dateFormat.parse(tmp);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		
		return date;
	}
	
	Long adjustLow(IndexRangeScan sc) {
		Long result = getDate(sc.rangeLow).getTime(); 
		if (sc.lowInclusive) {
			return result;
		}
		
		return result + 1;
	}
	
	Long adjustHigh(IndexRangeScan sc) {
		Long result = getDate(sc.rangeHigh).getTime(); 
		if (sc.highInclusive) {
			return result + 1;
		}
		
		return result;
	}
}
