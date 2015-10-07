package edu.buffalo.cse562.record;

import jdbm.PrimaryStoreMap;
import edu.buffalo.cse562.configurations.GlobalConfiguration;

public class ReadThread extends Thread {
	BoundedConcurrentLinkedQueue buffer;
    Table table;
    
    ReadThread(BoundedConcurrentLinkedQueue buffer, Table tbl) {
        this.buffer = buffer;
        this.table = tbl;
    }

    public void run() {
    	PrimaryStoreMap<Long, Row> storage = table.getStorage();
    	
    	try {
    		for (Row r:storage.values()) {
    			buffer.add(r);
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    		System.out.println("In table: " + table.tableName);
    		System.exit(1);
    	}
    	
    	if (GlobalConfiguration.debug){ 
    		System.out.println("Done table-- ");
    		Thread.currentThread().interrupt();
    	}
    }
}
