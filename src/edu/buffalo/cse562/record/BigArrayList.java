package edu.buffalo.cse562.record;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import edu.buffalo.cse562.configurations.GlobalConfiguration;

public class BigArrayList implements List<Row> {
	private final long CHUNK_SIZE = 200;
	private ArrayList<String> page_files = new ArrayList<String>();
	private boolean sorted;
	private ArrayList<Integer> sort_columns;
	private ArrayList<Boolean> ascs;
	private ObjectOutputStream cur_writer = null;
	private ArrayList<Row> cur_array = new ArrayList<Row>();
	
	public BigArrayList(boolean sorted, ArrayList<Integer> sort_columns, ArrayList<Boolean> ascs) {
		this.sorted = sorted;
		this.sort_columns = sort_columns;
		this.ascs = ascs;
		allocateNewPage();
	}
	
	public void allocateNewPage() {
		String p = GlobalConfiguration.getTmpFile();
		this.page_files.add(p);
		
		try {
			this.cur_writer = new ObjectOutputStream(new FileOutputStream(p));
			cur_array.clear();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean add(Row arg0) {
		
		if (cur_array.size() >= CHUNK_SIZE) {
			handleOverflow();
		}
	
		cur_array.add(arg0);		
		return false;
	}
	
	public void handleOverflow() {
		try {
			writeCurArrayList();
			cur_writer.close();
			allocateNewPage();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void writeCurArrayList() {
		try {
			if (sorted) {
				Collections.sort(cur_array, new RowComparator());
			}
			
			for (Row r: cur_array) {
				cur_writer.writeObject(r);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}		// TODO Auto-generated method stub
	
	public void flush() {
		try {
			writeCurArrayList();
			cur_array.clear();
			cur_writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void add(int arg0, Row arg1) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public boolean addAll(Collection<? extends Row> arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public boolean addAll(int arg0, Collection<? extends Row> arg1) {
		throw new IllegalArgumentException("Not supported");		
	}

	@Override
	public void clear() {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public boolean contains(Object arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		throw new IllegalArgumentException("Not supported");
	}
	
	class PageIterator {
		private ObjectInputStream br = null;
		public PageIterator(String page) {
			try {
				FileInputStream f = new FileInputStream(page);
				br = new ObjectInputStream(f);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		public Row getNext() {
			try {
				return (Row) br.readObject();
			} catch (EOFException e) {
				return null;
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
			return null;
		}
		
		public void stop() {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public class RowComparator implements Comparator<Row> {		
		@Override
	    public int compare(Row r1, Row r2) {
			int cmp = 0;
						
			for (int i=0; i<sort_columns.size(); i++) {
				int col = sort_columns.get(i);
				boolean isasc = ascs.get(i);
				Record m = r1.getRecord(col);
				Record n = r2.getRecord(col);
				cmp = m.compareTo(n);
	    	
				if (cmp == 0)
					continue;
				
	    		if (!isasc) {
	    			cmp = -cmp;
	    		}
	    		break;
			}
			
	    	return cmp;
	    }
	}
	
	ArrayList<PageIterator> pitrs = new ArrayList<PageIterator>();
	ArrayList<Row> row_saves = new ArrayList<Row>();
	
	public void preparePageIterators() {
		for (String page: page_files) {
			PageIterator pi = new PageIterator(page);
			pitrs.add(pi);
			row_saves.add(pi.getNext());
		}
	}
	
	public RowComparator rowComparator = new RowComparator();
	
	public Row pullSorted() {
		int i = 0;
		for (PageIterator p:pitrs) {
			Row r = row_saves.get(i);
			if (r == null) {
				row_saves.set(i, p.getNext());
			}
			i++;
		}
		
		int pos = -1;
		Row row_min = null;
		
		for (int j=0; j<row_saves.size(); j++) {
			Row r = row_saves.get(j);
			if (r != null) {
				if (row_min == null) {
					row_min = r;
					pos = j;
					continue;
				}
				if (rowComparator.compare(r, row_min) < 0) {
					row_min = r;
					pos = j;
				}
			}
		}
		
		if (pos != -1) {
			row_saves.set(pos,  null);
		}
			
		return row_min;
	}
	// TODO Auto-generated method stub
	@Override
	public int indexOf(Object arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public Iterator<Row> iterator() {
		return null;
	}

	@Override
	public int lastIndexOf(Object arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public ListIterator<Row> listIterator() {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public ListIterator<Row> listIterator(int arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public boolean remove(Object arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public Row remove(int arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public Row set(int arg0, Row arg1) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public int size() {
		throw new IllegalArgumentException("Not supported!");
	}

	@Override
	public List<Row> subList(int arg0, int arg1) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public Object[] toArray() {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		throw new IllegalArgumentException("Not supported");
	}

	@Override
	public Row get(int arg0) {
		return null;
	}	
}
