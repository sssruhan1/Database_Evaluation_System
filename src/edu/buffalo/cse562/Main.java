package edu.buffalo.cse562;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import edu.buffalo.cse562.RAOptimizer.PlanGenerator;
import edu.buffalo.cse562.RAOptimizer.Where2RangeScan;
import edu.buffalo.cse562.record.Record;
import edu.buffalo.cse562.record.Row;
import edu.buffalo.cse562.record.Table;
import edu.buffalo.cse562.configurations.GlobalConfiguration;
import edu.buffalo.cse562.evalRA.Evaluator;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.record.records.StringRecord;
import edu.buffalo.cse562.sql2RA.MyStatementVisitor;
import edu.buffalo.cse562.sql2RA.OpSelectCondition;
import edu.buffalo.cse562.sql2RA.OpTable;
import edu.buffalo.cse562.sql2RA.Operation;
import edu.buffalo.cse562.sql2RA.Sql2RA;

public class Main {
    public static void main(String [] args) {
    	if( args.length < 3){
			System.err.println("Usage: edu.buffalo.cse562.Main --data data_dir sql_file");
		}
		
		Statement stmt = null;
		DB db = new DB();
		
		String dir = null;
		RecordManager recman = null;
		for( int i = 0; i < args.length; i++){
			if(args[i].equals("--data")){
				dir = args[++i];
				continue;
			}
			
			if (args[i].equals("--index")) {
				GlobalConfiguration.using_index = true;
				GlobalConfiguration.index_path = args[++i];
				try {
					recman = RecordManagerFactory.createRecordManager(GlobalConfiguration.index_path + "/index");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(1);
				}
				continue;
			}
			
			if (args[i].equals("--build")) {
				GlobalConfiguration.build_index = true;
				// TODO: Short circuit
				continue;
			}
			
			if (args[i].equals("--swap")) {
				GlobalConfiguration.temporary_path = args[++i];
				// disable swap
				//GlobalConfiguration.needs_swap = true;
				continue;
			}
				
			try {
				BufferedReader stream = new BufferedReader(new FileReader(args[i]));
				String line;
				StringBuilder  stringBuilder = new StringBuilder();
			    try {
					while(( line = stream.readLine()) != null ) {
					        stringBuilder.append(lowerExceptQuote(line));
					        stringBuilder.append("\n");
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}
					

			    CCJSqlParser p = new CCJSqlParser(new StringReader(stringBuilder.toString()));
			    
				while( (stmt = p.Statement()) != null){
					MyStatementVisitor visitor = new MyStatementVisitor();
					stmt.accept(visitor);
					
					if (stmt instanceof CreateTable) {
						HashMap<String, List> table = visitor.getTables();
						
						for (String k:table.keySet()) {
							db.addTable(dir + "/" + k.toUpperCase() + ".dat", k.toLowerCase(), table.get(k));
							Table tbl = db.getTable(k.toLowerCase());
							tbl.createIndex(recman, k.toLowerCase());
						}
						recman.commit();
					} 
					
					if (stmt instanceof Insert) {
						Row row = new Row();
						Table tbl = db.getTable(visitor.getInsertTableName().toLowerCase());
						HashMap<String, Expression> insertions = visitor.getInsertItems();
						for (String k:insertions.keySet()) {
							Expression e = insertions.get(k);
							Record rcd = (Record) new Evaluator(db).evalExpression(e, row, tbl.schema);
							row.addRecord(rcd);
						}
						String pk = tbl.getPrimaryKey(row);
						
						Long n = tbl.table_storage.putValue(row);
						tbl.primaryKeyInverse.put(pk, n);
					}
					
					if (stmt instanceof Delete) {
						String table = visitor.getDelTableName();
						Expression ewhere = visitor.getDelWhere();
						OpSelectCondition op = new OpSelectCondition();
						op.setExpression(ewhere);
						OpTable opt = new OpTable();
						opt.setAlias(table);
						opt.setName(table);
						opt.setKidNull();
						op.setKid(opt);
						opt.setParent(op);
						Operation oproot = Where2RangeScan.tryConvert(op, opt);
						//optimizing query plan R.S
						//PlanGenerator pg = new PlanGenerator(op);
						// casting to OpSelectCondition is based on the assumption that
						// OpSelectCondition will always be the root of query plan in delete statement
						//OpSelectCondition oop =(OpSelectCondition)pg.getOptimal();
						// -end- R.S
						
						Table tbl = db.getTable(table);
						ArrayList<Row> rows = new Evaluator(db).evalSelection(oproot); // this probaly had to change to oop
						
					
						for (Row r:rows) {
							String pk = tbl.getPrimaryKey(r);
							Long key = tbl.primaryKeyInverse.get(pk);
							tbl.table_storage.remove(key);
							tbl.primaryKeyInverse.remove(pk);
						}
					}
					
					if (stmt instanceof Update) {
						long start = System.currentTimeMillis();
						long end;
						String table = visitor.getUpdateTableName();
						Expression ewhere = visitor.getUpdateWhere();
						HashMap<String, Expression> updateItems = visitor.getUpdateItems();
						OpSelectCondition op = new OpSelectCondition();
						op.setExpression(ewhere);
						OpTable opt = new OpTable();
						opt.setAlias(table);
						opt.setName(table);
						opt.setKidNull();
						op.setKid(opt);
						Table tbl = db.getTable(table);
						Evaluator eva = new Evaluator(db);
						
						Operation oproot = Where2RangeScan.tryConvert(op, opt);
						//optimizing query plan R.S
						// casting to OpSelectCondition is based on the assumption that
						// OpSelectCondition will always be the root of query plan in delete statement
						// OpSelectCondition oop =(OpSelectCondition)pg.getOptimal();
						// -end- R.S
						
						ArrayList<Row> rows = new Evaluator(db).evalSelection(oproot);// TODO: change op -> oop
						
						end = System.currentTimeMillis();
						if (GlobalConfiguration.debug) {
							System.out.println("Query in update use: " + (end - start));
						}
											
						HashMap<Integer, StringRecord> vals = null;
						GlobalConfiguration.update_index_keys = new ArrayList<String>();
						for (Row r:rows) {
							String pk = tbl.getPrimaryKey(r);
							Long key = tbl.primaryKeyInverse.get(pk);
							//tbl.primaryKeyInverse.remove(pk);
							//tbl.table_storage.remove(key);
							
							if (vals == null) {
								vals = new HashMap<Integer, StringRecord>();
								for (String field:updateItems.keySet()) {
									if (tbl.getIndex(field) != null) {
										GlobalConfiguration.update_index_keys.add(field);
									}
									Expression e = updateItems.get(field);
									StringRecord val = (StringRecord) eva.evalExpression(e, null, tbl.schema);
									int nc = tbl.schema.get(field);
									vals.put(nc, val);
									r.row.set(nc, val);
								}
							} else {
								for (int nc:vals.keySet()) {
									r.row.set(nc, vals.get(nc));
								}
							}
							
							//if (!needs_index_fix) { 
							tbl.table_storage.put(key, r);
							//}
							//else { 
                            //tbl.primaryKeyInverse.remove(pk);
							//tbl.table_storage.remove(key);
							//Long n = tbl.table_storage.putValue(r);
							//tbl.primaryKeyInverse.put(pk, n);
							//}
						}
						
						end = System.currentTimeMillis();
						if (GlobalConfiguration.debug) 
							System.out.println("Update in update use: " + (end - start));
						//recman.commit();
					}
					
					if (stmt instanceof Select && !GlobalConfiguration.build_index) {
						Operation root = visitor.getOpRoot();
						Evaluator e = new Evaluator(db);
						PlanGenerator pg = new PlanGenerator(root);
						Operation newRoot = pg.getOptimal();
						//newRoot = Where2RangeScan.tryMoveDownSelectCondition(newRoot);
						e.eval(newRoot);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (ParseException e){
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		try {
			recman.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
    }
    
    private static String getFileName(String s) {
    	String[] ss = s.split("/");
    	
    	if (GlobalConfiguration.debug){
    		System.out.println(ss[ss.length - 1]);
    	}
    	return ss[ss.length - 1];
    }
    
    private static String lowerExceptQuote(String s) {
    	String[] ss = s.split("'");
    	boolean flag = true;
    	String result = "";
    	for (String t:ss) {
    		if (flag) {
    			t = t.toLowerCase();
    			result += t;
    		} else {
    			result += "'" + t + "'";
    		}
    		
    		flag = !flag;
    	}
    	
    	return result;
    }
}
