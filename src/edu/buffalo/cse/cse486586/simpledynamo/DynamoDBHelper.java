package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class DynamoDBHelper extends SQLiteOpenHelper{

	public static final String DATABASE_NAME = "DynamoDB";
	public static final int DATABASE_VERSION = 1;
	public static final String TABLE_NAME = "provider";
	public static final String KEY = "key";
	public static final String VALUE = "value";
	public static final String REPLICA = "replica";

	private final String createTable = "create table " + TABLE_NAME + "("
			+ KEY + " TEXT, "
			+ VALUE + " TEXT, "
			+ REPLICA + " TEXT); ";
	private final String createUnique = "CREATE UNIQUE INDEX key_index ON provider (key); ";
	
	
	public DynamoDBHelper(Context context) {
	    super(context, DATABASE_NAME, null, DATABASE_VERSION);
	}
	
	@Override
	public void onCreate(SQLiteDatabase arg0) {
		arg0.execSQL(createTable);		
		arg0.execSQL(createUnique);
	}

	@Override
	public void onUpgrade(SQLiteDatabase arg0, int arg1, int arg2) {
		// TODO Auto-generated method stub
		
	}

}
