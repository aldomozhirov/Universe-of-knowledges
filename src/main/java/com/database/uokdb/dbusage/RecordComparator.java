package com.database.uokdb.dbusage;

import java.util.Comparator;
import java.util.HashMap;

public class RecordComparator implements Comparator<HashMap<String, Object>> {

	private String fieldToCompare;
	private boolean ascending;

	public RecordComparator(String fieldToCompare, boolean ascending) {
		this.fieldToCompare = fieldToCompare;
		this.ascending = ascending;
	}

	public int compare(HashMap<String, Object> first, HashMap<String, Object> second) {

		if (!ascending)
			return ((String) first.get(fieldToCompare)).compareTo((String) second.get(fieldToCompare));
		else
			return -1 * ((String) first.get(fieldToCompare)).compareTo((String) second.get(fieldToCompare));

	}
}
