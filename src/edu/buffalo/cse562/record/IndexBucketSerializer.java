package edu.buffalo.cse562.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import edu.buffalo.cse562.record.records.DoubleRecord;
import edu.buffalo.cse562.record.records.LongRecord;
import edu.buffalo.cse562.record.records.StringRecord;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

public class IndexBucketSerializer implements Serializer<IndexBucket> {
	public IndexBucketSerializer() {
	}
		
	@Override
	public IndexBucket deserialize(SerializerInput in) throws IOException,
			ClassNotFoundException {
		IndexBucket bucket = new IndexBucket();
		int size = in.readInt();
		try {
			for (int i=0; i<size; i++) {
				long l = in.readLong();
				bucket.putRow(l);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return bucket;
	}

	@Override
	public void serialize(SerializerOutput out, IndexBucket bucket) throws IOException {
		int size = bucket.getSize();
		out.writeInt(size);
		
		for (Long l:bucket.getRows()) {
			out.writeLong(l);
		}
	}
}
