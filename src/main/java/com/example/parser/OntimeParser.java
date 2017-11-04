package com.example.parser;

import org.apache.hadoop.io.Text;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OntimeParser {

	private Integer year;
	private String uniquecarrier;
	
public OntimeParser(Text text) {
	String[] columns = text.toString().split(",");
	
	year = Integer.parseInt(columns[0]);
	uniquecarrier = columns[8];
	}
}
