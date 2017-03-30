package com.database.uokdb.dbusage;

import com.database.uokdb.SQLParser.SQLOperationsTypes.CompareType;
import com.database.uokdb.db.BTreeMap;
import com.database.uokdb.db.DB;

import java.util.*;

public class Table {

	// join(), groupBy()
	// Костыльная индексация

	private BTreeMap<Integer, HashMap<String, Object>> data;
	private String name;
	private String[] fieldNames;
	private DB db;

	public Table(DB db, String name, boolean commit, String... fieldNames) {
		this.name = name;
		this.db = db;
		data = db.treeMap(name);

		this.fieldNames = fieldNames;
		if (commit)
			db.commit();
	}

	private List<HashMap<String, Object>> collectionToList(Collection<HashMap<String, Object>> records) {
		ArrayList<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		for (HashMap<String, Object> record : records)
			list.add(record);
		return list;
	}

	public List<HashMap<String, Object>> getData() {
		return collectionToList(data.values());
	}

	public String getKey() {
		return fieldNames[0];
	}

	public String getName() {
		return name;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public String getNewId() {
		try {
			return Integer.toString(data.lastKey() + 1);
		} catch (Exception e) {
			return Integer.toString(1);
		}
	}

	private boolean isRecordValid(int length) {
		if (length == fieldNames.length)
			return true;
		System.out.println("Wrong number of values: " + length);
		return false;
	}

	private HashMap<String, Object> arrayToHashMap(String[] array) {

		HashMap<String, Object> record = new HashMap<String, Object>();

		for (int i = 0; i < array.length; i++)
			record.put(fieldNames[i], array[i]);

		return record;
	}

	private HashMap<String, Object> recordAddition(HashMap<String, Object> fRecord, String fTableName,
												   HashMap<String, Object> sRecord, String sTableName) {

		HashMap<String, Object> resRecord = new HashMap<String, Object>();

		for (Map.Entry<String, Object> pair : fRecord.entrySet())
			if (pair.getKey().contains("."))
				resRecord.put(pair.getKey(), pair.getValue());
			else
				resRecord.put(fTableName + "." + pair.getKey(), pair.getValue());

		for (Map.Entry<String, Object> pair : sRecord.entrySet())
			if (pair.getKey().contains("."))
				resRecord.put(pair.getKey(), pair.getValue());
			else
				resRecord.put(sTableName + "." + pair.getKey(), pair.getValue());

		return resRecord;
	}

	public Table selectWhere(String fField, String sField, CompareType q) {

		Table result = new Table(db, this.name + "228", false, fieldNames);
		result.data.clear();
		for (HashMap<String, Object> record : data.values())
			if (((String) record.get(fField)).equals(((String) record.get(sField))))
				result.insert(record);

		return result;
	}

	public Table cartesian(Table table) {

		String[] fieldNames = new String[this.fieldNames.length + table.fieldNames.length + 1];

		fieldNames[0] = "tempId";

		for (int i = 1; i < this.fieldNames.length; i++)
			fieldNames[i] = this.name + "." + this.fieldNames[i];

		for (int i = 1; i < table.fieldNames.length; i++)
			fieldNames[i + this.fieldNames.length] = table.name + "." + table.fieldNames[i];

		Table result = new Table(db, table.name + "Join" + this.name, false, fieldNames);
		result.data.clear();
		HashMap<String, Object> newRecord = new HashMap<String, Object>();

		for (HashMap<String, Object> recordOne : this.data.values()) {
			for (HashMap<String, Object> recordTwo : table.data.values()) {

				newRecord = recordAddition(recordOne, this.name, recordTwo, table.name);
				newRecord.put("tempId", result.getNewId());
				result.insert(newRecord);
				newRecord = new HashMap<String, Object>();
			}
		}

		return result;
	}

	public void insert(String...values) {

		if (!isRecordValid(values.length))
			return;

		HashMap<String, Object> record = arrayToHashMap(values);

		// System.out.println(data.get(record.get(fieldNames[0])));

		if (data.get(Integer.parseInt(record.get(fieldNames[0]).toString())) == null) {
			data.put(Integer.parseInt(record.get(fieldNames[0]).toString()), record);
			db.commit();
		} else
			System.out.println("Record associated with " + values[0] + " already exists.");
	}

	private void insert(HashMap<String, Object> record) {

		if (record == null || !isRecordValid(record.size()))
			return;

		if (data.get(Integer.parseInt(record.get(fieldNames[0]).toString())) == null)
			data.put(Integer.parseInt(record.get(fieldNames[0]).toString()), record);
	}

	public void update(String...values) {

		if (!isRecordValid(values.length))
			return;

		HashMap<String, Object> record = arrayToHashMap(values);

		if (data.get(Integer.parseInt(record.get(fieldNames[0]).toString())) != null) {
			data.remove(Integer.parseInt(values[0]));
			data.put(Integer.parseInt(record.get(fieldNames[0]).toString()), record);
			db.commit();
		} else
			System.out.println("Record associated with " + values[0] + " doesn't exists.");
	}

	public String toString() {
		return data.values().toString();
	}

	boolean compare(String value1, String value2, CompareType q) {
		switch (q) {
			case Eq:
				return value1.equals(value2);
			case Less:
				return value1.compareTo(value2) == -1;
			case LessEq:
				return value1.equals(value2) || value1.compareTo(value2) == -1;
			case More:
				return value1.compareTo(value2) == 1;
			case MoreEq:
				return value1.equals(value2) || value1.compareTo(value2) == 1;
			case NotEq:
				return !value1.equals(value2);
			default:
				return false;
		}
	}

	public Table select(String fieldName, String value, CompareType q) {

		Table result = new Table(db, name + "Select", false, fieldNames);
		result.data.clear();
		if (fieldName.equals(fieldNames[0])) {
			result.insert(data.get(Integer.parseInt(value)));
			return result;
		}

		for (HashMap<String, Object> record : data.values())
			if (compare((String) record.get(fieldName), value, q))
				result.insert(record);

		return result;
	}

	public Table selectDistinct(String fieldName, String value, String... resultFields) {
		if (!this.fieldNames[0].equals(resultFields[0])) {
			String newResFields[] = new String[resultFields.length + 1];
			newResFields[0] = "id";
			for (int i = 1; i < newResFields.length; i++)
				newResFields[i] = resultFields[i - 1];
			resultFields = newResFields;
		}
		Table result = new Table(db, name + "SelectDistinct", false, resultFields);
		result.data.clear();
		if (resultFields.length > fieldNames.length) {
			System.out.println("This table has only " + fieldNames.length + "fields.");
			return result;
		}

		HashMap<String, Object> resRecord = new HashMap<String, Object>();

		for (HashMap<String, Object> record : data.values())
			if (record.get(fieldName).toString().equals(value)) {
				resRecord = new HashMap<String, Object>();
				resRecord.put(result.getKey(), result.getNewId());
				for (String resultField : resultFields) {
					resRecord.put(resultField, record.get(resultField));
				}
				if (!result.data.containsValue(resRecord))
					result.insert(resRecord);
			}

		return result;

	}

	public Table project(String fieldName, String value, CompareType q, String... resultFields) {

		boolean createId = false;

		if (!this.fieldNames[0].equals(resultFields[0])) {
			createId = true;
			String newResFields[] = new String[resultFields.length + 1];
			newResFields[0] = "tempId";
			for (int i = 1; i < newResFields.length; i++)
				newResFields[i] = resultFields[i - 1];
			resultFields = newResFields;
		}

		Table result = new Table(db, name + "Project", false, resultFields);
		result.data.clear();
		if (resultFields.length > fieldNames.length) {
			System.out.println("This table has only " + fieldNames.length + "fields.");
			return result;
		}

		for (HashMap<String, Object> record : data.values())
			if (compare((String) record.get(fieldName), value, q)) {
				HashMap<String, Object> resRecord = new HashMap<String, Object>();
				for (int i = 1; i < resultFields.length; i++) {

					resRecord.put(resultFields[i], record.get(resultFields[i]));
				}
				if (createId) {
					resRecord.put("tempId", result.getNewId());
				}
				result.insert(resRecord);
			}

		return result;
	}

	public ArrayList<HashMap<String, Object>> project(String... resultFields) {

//		boolean createId = false;

		// if (!this.fieldNames[0].equals(resultFields[0])) {
		// createId = true;
		// String newResFields[] = new String[resultFields.length + 1];
		// newResFields[0] = "tempId";
		// for (int i = 1; i < newResFields.length; i++)
		// newResFields[i] = resultFields[i - 1];
		// resultFields = newResFields;
		// }

//		Table result = new Table(db, name + "Project", false, resultFields);

		// if (resultFields.length > fieldNames.length) {
		// System.out.println("This table has only " + fieldNames.length +
		// "fields.");
		// return result;
		// }

		HashMap<String, Object> resRecord = new HashMap<String, Object>();

		ArrayList<HashMap<String, Object>> vsoPropalo = new ArrayList<HashMap<String, Object>>();

		for (HashMap<String, Object> record : data.values()) {
			resRecord = new HashMap<String, Object>();
			for (int i = 0; i < resultFields.length; i++)

				resRecord.put(resultFields[i], record.get(resultFields[i]));
			// if (createId) {
			// resRecord.put("tempId", result.getNewId());
			// }
			// result.insert(resRecord);
			vsoPropalo.add(resRecord);
		}
		return vsoPropalo;
	}

	public ArrayList<HashMap<String, Object>> sortBy(String fieldName, boolean ascending) {

		ArrayList<HashMap<String, Object>> data = new ArrayList<HashMap<String, Object>>();

		for (HashMap<String, Object> record : this.data.values())
			data.add(record);

		data.sort(new RecordComparator(fieldName, false));
		return data;
	}

	public void delete(String fieldName, String value) {

		if (fieldName.equals(fieldNames[0])) {
			data.remove(Integer.parseInt(value));
			db.commit();
			return;
		}

		for (HashMap<String, Object> record : data.values()) {
			if (record.get(fieldName).equals(value))
				data.remove(Integer.parseInt(record.get(fieldNames[0]).toString()));
		}
		db.commit();
	}
}