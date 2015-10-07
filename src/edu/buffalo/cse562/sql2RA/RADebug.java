package edu.buffalo.cse562.sql2RA;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import edu.buffalo.cse562.sql2RA.Operation;

public class RADebug {
	private int allocator = 0;
	private String outputFile;
	private PrintWriter writer;
	private HashMap<Operation, Integer> nodes = new HashMap<Operation, Integer>();
	
	public RADebug(String output) {
		this.outputFile = output;
	}
	
	public void emitNodeDefinition(Operation op) {
		if (!nodes.containsKey(op)) {
			Integer n =new Integer(allocator);
			nodes.put(op, n);
			allocator++;
			String additional_info = "";
			if (op instanceof OpTable) {
				additional_info = (String)((OpTable)op).getAlias();
			} else {
				if (op instanceof OpTarget) {
					for (Object o: ((OpTarget)op).getTargetList()) {
						additional_info += (o.toString() + " ");
					}
				} 
				else {
					additional_info = (op.getExpression() != null) ? 
							(op.getExpression().toString()) : " ";
					additional_info = (op.getAnnotation() != null) ?
									(additional_info + " annotation: '" + op.getAnnotation().idxType + "' ") :
										additional_info;
				}
			}
			writer.println(n.toString()+" [label=\""+op.getOpName()+ " " +
					additional_info + "\"];");
		}
	}
	
	public void emitNodeConnection(Operation start, Operation end) {
		Integer n1 = nodes.get(start);
		Integer n2 = nodes.get(end);
		writer.println(n1.toString() + "->" + n2.toString() + ";");
	}
	
	public void debugRATree(Operation op) {
		try {
			this.writer = new PrintWriter("tmp.dot", "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		writer.println("digraph G {");
		emitNodeDefinition(op);
		doDebugRATree(op);
		writer.println("}");
		writer.close();
		try {
			Runtime.getRuntime().exec("dot -Tpng tmp.dot -o " + outputFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void doDebugRATree(Operation op) {
		Operation left = op.getLeft();
		Operation right = op.getRight();
		
		if (left != null) {
			emitNodeDefinition(left);
			emitNodeConnection(op, left);
			doDebugRATree(left);
		}
		
		if (right != null) {
			emitNodeDefinition(right);
			emitNodeConnection(op, right);
			doDebugRATree(right);
		}
	}
}