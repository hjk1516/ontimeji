package com.example.datekey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator {
	protected DateKeyComparator() {
		super(DateKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		
		DateKey k1 = (DateKey) w1;
		DateKey k2 = (DateKey) w2;
		
		int cmp = k1.getYear().compareTo(k2.getYear());
		if (cmp != 0) {
			return cmp;
		} 
		if (k1.getUniquecarrier() == k2.getUniquecarrier())
			cmp = k1.getUniquecarrier().compareTo(k2.getUniquecarrier());
		return cmp;
		
		
	}
}
