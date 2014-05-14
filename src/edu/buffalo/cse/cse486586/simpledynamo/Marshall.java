package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.HashMap;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;

public class Marshall {
	
	public static synchronized HashMap<String,String> contentValueToHashMap(ContentValues values) {
		if (values == null) {
			return null;
		}
		else {
			HashMap<String,String> contentMap = new HashMap<String,String>();
			for (String key : values.keySet()) {
				contentMap.put(key, values.getAsString(key));
			}
			return contentMap;
		}
	}
	
	public static synchronized ContentValues hashMapToContentValue(HashMap<String,String> contentMap) {
		if (contentMap == null) {
			return null;
		}
		else {
			ContentValues values = new ContentValues();
			for (String key : contentMap.keySet()) {
				values.put(key, contentMap.get(key));
			}
			return values;
		}
	}

	public static synchronized HashMap<String,String> cursorToHashMap(Cursor cursor) {
		if (cursor != null) {
			HashMap<String,String> contentMap = new HashMap<String,String>();
		    if (cursor.moveToFirst()) {
		        do {
		            contentMap.put(cursor.getString(0),cursor.getString(1));
		        } while (cursor.moveToNext());
		    } 
		    if (cursor != null && !cursor.isClosed()) {
		        cursor.close();
		    }
			return contentMap;
		}
		return null;
	}
	
	public static synchronized Cursor hashMaptoCursor(HashMap<String,String> contentMap) {
		if (contentMap != null) {
			MatrixCursor matrixCursor = new MatrixCursor(new String[] { "key", "value" });
			for (String key: contentMap.keySet()) {
				matrixCursor.addRow(new Object[] { key, contentMap.get(key) });
			}
			return matrixCursor;
		}
		return null;
	}

	public static synchronized HashMap<String,String> addHashMapToCursor(HashMap<String,String> contentMap, Cursor cursor) {
		if (contentMap != null && cursor != null) {
			MatrixCursor matrixCursor = new MatrixCursor(new String[] { "key", "value" });
			for (String key: contentMap.keySet()) {
				matrixCursor.addRow(new Object[] { key, contentMap.get(key) });
			}
			MergeCursor mergeCursor = new MergeCursor(new Cursor[] { matrixCursor, cursor });
			return Marshall.cursorToHashMap(mergeCursor);
		}
		else if (contentMap != null && cursor == null) {
			return contentMap;
		}
		else if (contentMap == null && cursor != null) {
			return Marshall.cursorToHashMap(cursor);
		}
		return null;
	}
	
	public static synchronized Cursor addCursorToCursor(Cursor cursor1, Cursor cursor2) {
		if (cursor1 == null && cursor2 == null) {
			return null;
		}
		else if (cursor1 != null && cursor2 == null) {
			return cursor1;
		}
		else if (cursor1 == null && cursor2 != null) {
			return cursor2;
		}
		else if (cursor1 != null && cursor2 != null) {
			if (cursor1.getCount() == 0) {
				return cursor2;
			}
			else if (cursor2.getCount() == 0) {
				return cursor1;
			}
			else {
				MergeCursor mergedCursor = new MergeCursor(new Cursor[] {cursor1,cursor2});
				return (Cursor) mergedCursor;
			}
		}
		return null;
	}
	
	public static synchronized String[] printCursor(Cursor cursor) {
		String[] rows = new String[200];
		int i = 0;
	    if (cursor.moveToFirst()) {
	        do {
	            rows[i] = cursor.getString(0);
	            rows[i] = cursor.getString(1);
	            i++;
	        } while (cursor.moveToNext());
	    }
	    if (cursor != null && !cursor.isClosed()) {
	        cursor.close();
	    }
	    return rows;
	}
}