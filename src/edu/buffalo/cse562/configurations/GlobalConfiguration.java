package edu.buffalo.cse562.configurations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class GlobalConfiguration {
	public static String temporary_path = "";
	public static String group_by_field = "";
	public static boolean needs_swap = false;
	public static HashSet<String> group_by_set = null;
	public static HashMap<String, String> table_alias = new HashMap<String, String>();
	public static boolean debug = false;
	private static int file_counter = 0;
	private static int cnt = 1;
	public static int[] ids = new int[2];
	public static boolean limit_reached = false;
	public static String index_path = "";
	public static boolean build_index = false;
	public static boolean using_index = false;
	public static boolean isPushSelect = true;
	public static boolean isIndexWisePushSelect = true;
	public static ArrayList<String> update_index_keys = null;
	public static void set_group_by_set(HashSet<String> st) {
		group_by_set = st;
	}
	
	public static void addTableAlias(String alias, String name) {
		table_alias.put(alias,  name);
	}
	
	public static String getTableAlias(String alias) {
		return table_alias.get(alias);
	}
	
	synchronized public static String getTmpFile() {
		return temporary_path + "/" + "tmp_" + (file_counter++);
	}
    synchronized public static String getFile(Double d){
		file_counter++;
		return temporary_path + "/" + "tmp_" + Double.toString(d);
	}
    
    synchronized public static Integer tryGet(String k) {
    	Integer key = Integer.parseInt(k);
    	if (ids[key] == 0) {
    		return null;
    	}
    	
    	return ids[key];
    }
	
	synchronized public static Integer getKey(String k) {
		Integer key = Integer.parseInt(k);
		if (ids[key] == 0) {
			ids[key] = cnt++;
		}
		return ids[key];
	}
	
	synchronized public static void set_limit() {
		limit_reached = true;
	}
	
	synchronized public static boolean limit_reached() {
		return limit_reached;
	}
}
