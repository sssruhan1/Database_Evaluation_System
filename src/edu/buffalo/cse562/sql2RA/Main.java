package edu.buffalo.cse562.sql2RA;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.buffalo.cse562.evalRA.Evaluator;
import edu.buffalo.cse562.record.DB;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class Main {


	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if( args.length < 3){
			System.err.println("Usage: edu.buffalo.cse562.Main --data data_dir sql_file");
		}
		String dir = null;
		for( int i = 0; i < args.length; i++){
			if(args[i].equals("--data")){
				dir = args[++i];
			}
			else{
				try {
					FileReader stream = new FileReader(args[i]);
					CCJSqlParser p = new CCJSqlParser(stream);
					Statement stmt = null;
					DB db = new DB();
					while( (stmt = p.Statement()) != null){
						MyStatementVisitor visitor = new MyStatementVisitor();
						stmt.accept(visitor);
						if (stmt instanceof Insert){
							System.out.println("insert item:" + visitor.getInsertItems().toString());
							System.out.println("from table: " + visitor.getInsertTableName());
						}
						if (stmt instanceof Update){
							System.out.println("update item:" + visitor.getUpdateItems().toString());
							System.out.println("in table:" + visitor.getUpdateTableName());
							System.out.println("where " + visitor.getUpdateWhere().toString());
						}
						if (stmt instanceof Delete){
							System.out.println("del from table:" + visitor.getDelTableName());
							System.out.println("where: " + visitor.getDelWhere().toString());
						}
						
						if (stmt instanceof CreateTable) {
							HashMap<String, List> table = visitor.getTables();
							for (String k:table.keySet()) {
								System.out.println(k);
								db.addTable(dir + "/" + k + ".dat", k, table.get(k));
							}
						} 
						
						if (stmt instanceof Select) {
							Operation root = visitor.getOpRoot();
							Evaluator e = new Evaluator(db);
							e.eval(root);
							//RAEvaluator rae = new RAEvaluator(db);
							//rae.eval(root);
						}
					}

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (ParseException e){
					e.printStackTrace();
				}
			}
		}
	}
}
