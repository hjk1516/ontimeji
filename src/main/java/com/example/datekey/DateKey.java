package com.example.datekey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DateKey implements WritableComparable<DateKey> {
	
	private Integer year;
	private String uniquecarrier;
	
	public DateKey() {
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		uniquecarrier = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		WritableUtils.writeString(out, uniquecarrier);
		}

	@Override
	public int compareTo(DateKey key) {
		int result = year.compareTo(key.year);
		if (0 == result) {
			result = uniquecarrier.compareTo(key.uniquecarrier);
		}
		return result;
	}
	
	@Override
	public String toString() {
		return year + "," + uniquecarrier;
	}

}
