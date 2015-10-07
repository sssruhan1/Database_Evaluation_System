package edu.buffalo.cse562.RAOptimizer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import edu.buffalo.cse562.evalRA.Evaluator;
import edu.buffalo.cse562.record.DB;
import edu.buffalo.cse562.sql2RA.MyStatementVisitor;
import edu.buffalo.cse562.sql2RA.Operation;

public class OpMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
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
					while( (stmt = p.Statement()) != null){
						MyStatementVisitor visitor = new MyStatementVisitor();
						stmt.accept(visitor);
						
						if (stmt instanceof Select) {
							Operation root = visitor.getOpRoot();
							PlanGenerator pg = new PlanGenerator(root);
							pg.getOptimal();
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
