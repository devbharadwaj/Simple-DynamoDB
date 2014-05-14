package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.buffalo.cse.cse486586.simpledynamo.CircularLinkedList.Node;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	
	private static final String AUTH = "edu.buffalo.cse.cse486586.simpledynamo";
	private static final Uri MESSAGES_URI = Uri.parse("content://"+AUTH+"."+DynamoDBHelper.TABLE_NAME);
	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private static final int SERVER_PORT = 10000;
	private static final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
	private static final Lock readLock = readWriteLock.readLock();
	private static final Lock writeLock = readWriteLock.writeLock();
	private static boolean noInsertResponse = true;
	private static boolean noDeleteResponse = true;
	private static boolean noQueryResponse = true;
	private static boolean recovering = true;
	private static List<Boolean> timers;
	private static List<Cursor> cursorList;
	private static List<String> deadLockKeys;
	private static List<Messages> bufferedMessages;
	private static int hackAt = 0;
	private ServerTask serverTask;
	private DynamoNode dynamoNode;
	private RecoverNode recoverNode;
	SQLiteDatabase readdb;
	SQLiteDatabase writedb;
	DynamoDBHelper dbHelper;
	
	@Override
	public boolean onCreate() {
		serverTask = new ServerTask(SERVER_PORT);
		dynamoNode = new DynamoNode(this.getNodeId());
		dbHelper = new DynamoDBHelper(getContext());
		readdb = dbHelper.getReadableDatabase();
		writedb = dbHelper.getWritableDatabase();
		recovering = true;
		this.timers = Collections.synchronizedList(new ArrayList<Boolean>());
		this.cursorList = Collections.synchronizedList(new ArrayList<Cursor>());
		this.deadLockKeys = Collections.synchronizedList(new ArrayList<String>());
		this.bufferedMessages = Collections.synchronizedList(new ArrayList<Messages>());
		recoverNode = new RecoverNode();
		return false;
	}
	/*
	 * Externally called delete, relays delete message to Quorum
	 */
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		
		if (selection.equals("*")) {
			LinkedHashMap<String,String[]> nodeList = dynamoNode.allPeers.getHashedNodeList();
			for (String[] destNode : nodeList.values()) {
				new DeleteMessage(dynamoNode.nodeID, destNode[0], "*", "*", null);
				waitForDelete();
			} 
		}
		else if (selection.equals("@")) {
			new DeleteMessage(dynamoNode.nodeID, dynamoNode.nodeID, "@", dynamoNode.nodeID, null);
			waitForDelete();
			for (String sendTo : dynamoNode.quorumPeers){
				new DeleteMessage(dynamoNode.nodeID, sendTo, "@", dynamoNode.nodeID, null);
				waitForDelete();
			}
		}
		else {
			sendDeleteCoordinator(SHA1.genHash(selection), selection);
		}
		return 0;
	}
	/*
	 * Internal called delete, does the actual deletion
	 */
	public int deleteOperator(Uri uri, String selection, String replica) {
		writeLock.lock();
		if (selection.equals("*")) {
			writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME);
			Log.v("Delete", "Delete *");
			//writedb.close();
			writeLock.unlock();
			return 1;
		}
		else if (selection.equals("@")) {
			writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE replica = '"+replica+"'");
			Log.v("Delete", "Delete @");
			//writedb.close();
			writeLock.unlock();
			return 1;
		}
		else {
			writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE key = '"+selection+"' AND replica = '"+replica+"'");
			Log.v("Delete", "Delete " + selection);
			//writedb.close();
			writeLock.unlock();
			return 1;
		}
		
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}
	/*
	 * Externally called insert, relays insert message to Quorum
	 */

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = values.getAsString("key");
		String keyHash = SHA1.genHash(key);
		sentInsertCoordinator(keyHash,values);
		return uri;
	}
	
	/*
	 * Internally called insert, does the actual insertion 
	 */
	public Uri insertOperator(Uri uri, ContentValues values) {
		
		String key = values.getAsString("key");
		if (!deadLockKeys.contains(key)) {
			deadLockKeys.add(key);
		}
		else {
			try {
				while(deadLockKeys.contains(key)) {
					Thread.sleep(10);
				}
				deadLockKeys.add(key);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
    	SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
    	queryBuilder.setTables(DynamoDBHelper.TABLE_NAME);
		queryBuilder.appendWhere("key='"+key+"'");
		Cursor cursor = queryBuilder.query(readdb, null, null, null, null, null, null);
		
		if (cursor.getCount() > 0) {
			//values.put("version", String.valueOf(cursor.getCount()+1));
			System.out.println("Deleted Key:"+key);
			//writedb.delete(DynamoDBHelper.TABLE_NAME,"key='"+key+"'", null);
			//writedb.insert(DynamoDBHelper.TABLE_NAME, null, values);
			writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE key = '"+key+"'");
			writedb.replace(DynamoDBHelper.TABLE_NAME, null, values);
			Log.v("insert", values.toString());
		}
		else {
			writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE key = '"+key+"'");
			writedb.replace(DynamoDBHelper.TABLE_NAME, null, values);
			Log.v("insert", values.toString());
		}
		//readdb.close();
		//writedb.close();
		deadLockKeys.remove(key);
		return uri;
	}
	
	/*
	 * Externally called Query, relays message to Quorum
	 */
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
    	queryBuilder.setTables(DynamoDBHelper.TABLE_NAME);

		
		if (selection.equals("*")) {
			Log.v("Query","*");
	    	int sequenceNo;
	    	int cursorNo;
			Cursor responseCursor = null;
			LinkedHashMap<String,String[]> nodeList = dynamoNode.allPeers.getHashedNodeList();
			for (String[] destNode : nodeList.values()) {
				sequenceNo = timers.size();
				timers.add(true);
				cursorNo = cursorList.size();
				cursorList.add(null);
				new QueryMessage(dynamoNode.nodeID, destNode[0], cursorNo, sequenceNo, selection, destNode[0], null, null).send();
				waitForQuery(sequenceNo);
				if (cursorList.get(cursorNo) == null) {
					sequenceNo = timers.size();
					timers.add(true);
					new QueryMessage(dynamoNode.nodeID, destNode[1], cursorNo, sequenceNo, selection, destNode[0], null, null).send();
					waitForQuery(sequenceNo);
				}
				if (cursorList.get(cursorNo) == null) {
					sequenceNo = timers.size();
					timers.add(true);
					new QueryMessage(dynamoNode.nodeID, destNode[2], cursorNo, sequenceNo, selection, destNode[0], null, null).send();
					waitForQuery(sequenceNo);
				}
				responseCursor = Marshall.addCursorToCursor(cursorList.get(cursorNo), responseCursor);
			} 
			//Cursor mycursor = queryBuilder.query(readdb, new String[] {"key","value"}, "replica='"+dynamoNode.nodeID+"'", null, null, null, null);
			//responseCursor = Marshall.addCursorToCursor(mycursor, responseCursor);
			return responseCursor;
		}
		else if (selection.equals("@")) {
			Log.v("Query", "@");
			readLock.lock();
			Cursor cursor = null;
			hackAt++;
/*			Cursor cursor = queryBuilder.query(readdb, null,
											   " replica ='"+dynamoNode.nodeID+"' AND version = (select max(version) from provider p where p.key = provider.key) ",
											   null, null, null, null);
*/			//String sql = "SELECT key,value FROM provider WHERE replica = '"+dynamoNode.nodeID+"' and  version = (select max(version) from provider p where p.key = provider.key)";
			cursor = queryBuilder.query(readdb, new String[] {"key","value"}, null, null, null, null, null);
			//if (hackAt % 2 == 0 && oldvalue == newvalue) {
			//	this.cleanTheDataBase();
			//}
			readLock.unlock();
			return cursor;
		}
		else {
			
			synchronized (this) {
				String hash = SHA1.genHash(selection);
	 			Log.v("Query", "GeneralQ Key: "+selection+" KeyHash: " + hash);
				Cursor cursor = queryBuilder.query(readdb, new String[] {"key","value"}, "key ='"+selection+"'", null, null, null, null);
				if (cursor.getCount() > 0) {
					Log.v("Query", "QueryAnswered Locally");
					return cursor;
				}			
			}
			if (!deadLockKeys.contains(selection)) {
				//deadLockKeys.add(selection);
			}
			else {
				try {
					while(deadLockKeys.contains(selection)) {
						Thread.sleep(10);
					}
					//deadLockKeys.add(selection);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			String keyHash = SHA1.genHash(selection);
			int cursorNo;
			cursorNo = cursorList.size();
			cursorList.add(null);
			sentQueryCoordinator(keyHash,selection,cursorNo);
			//deadLockKeys.remove(selection);
			return cursorList.get(cursorNo);
		}
		
	}
	
	/*
	 * Internally called Query, does the actual query
	 */
	public Cursor queryOperator(Uri uri, String selection, String replica) {
    	SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
    	queryBuilder.setTables(DynamoDBHelper.TABLE_NAME);
		
		if (selection.equals("*")) {
			readLock.lock();
			Cursor cursor = queryBuilder.query(readdb, new String[] {"key","value"}, "replica='"+replica+"'", null, null, null, null);
			Log.v("Query", "* Query, Tuples:"+ cursor.getCount());
			readLock.unlock();
			return cursor;
		}
		else if (selection.equals("@")) {
			/*
			 * Implemented in external query
			 */
		}
		else {
			if (!deadLockKeys.contains(selection)) {
				deadLockKeys.add(selection);
			}
			else {
				try {
					while(deadLockKeys.contains(selection)) {
						Thread.sleep(10);
					}
					deadLockKeys.add(selection);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			Cursor cursor = queryBuilder.query(readdb, new String[] {"key","value"}, "key='"+selection+"' AND replica='"+replica+"'", null, null, null, null);
			Log.v("Query", "Tuples:"+ cursor.getCount());
			deadLockKeys.remove(selection);
			return cursor;
		}
		//readdb.close();
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/*
	 * Get Node ID
	 */
    private String getNodeId() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String node = String.valueOf((Integer.parseInt(portStr)));
        return node;
    }
    public void cleanTheDataBase() {
    	writedb.delete(DynamoDBHelper.TABLE_NAME,null,null);
    	long numRows = DatabaseUtils.queryNumEntries(readdb, DynamoDBHelper.TABLE_NAME);
    	System.out.println("Number of rows: "+numRows);
    }
    class CleanDB extends Thread{
    	
    	CleanDB() {
    		this.start();
    	}
    	
    	public void run() {
    		try {
				Thread.sleep(50);
				cleanTheDataBase();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
    }
    public void computeBuffer() {
    	InsertMessage insertMessage;
    	QueryMessage queryMessage;
    	while (!bufferedMessages.isEmpty()) {
    		Messages theMessage = bufferedMessages.remove(0);
    		if (theMessage instanceof InsertMessage) {
    			insertMessage = (InsertMessage) theMessage;
				String key = "Bogus";
				for(String keyed: insertMessage.values.keySet()) {
					key = keyed;
					break;
				}
		    	SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		    	queryBuilder.setTables(DynamoDBHelper.TABLE_NAME);
				queryBuilder.appendWhere("key="+"'"+key+"'");
				Cursor cursor = queryBuilder.query(readdb, null, null, null, null, null, null);
				if (cursor.getCount() > 0) {
					Log.v("Deleted Key:", key);
					//writedb.delete(DynamoDBHelper.TABLE_NAME,"key='"+key+"'", null);
					//writedb.insert(DynamoDBHelper.TABLE_NAME, null, Marshall.hashMapToContentValue(insertMessage.values));
					writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE key = '"+key+"'");
					writedb.replace(DynamoDBHelper.TABLE_NAME, null, Marshall.hashMapToContentValue(insertMessage.values));
					Log.v("Cache-insert", insertMessage.values.get(key));
				}
				else {
					writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE key = '"+key+"'");
					writedb.replace(DynamoDBHelper.TABLE_NAME, null, Marshall.hashMapToContentValue(insertMessage.values));
					Log.v("Cache-insert", insertMessage.values.get(key));
				}
				new InsertMessage(Integer.toString(insertMessage.destNode), Integer.toString(insertMessage.sourNode), insertMessage.sequenceNo, null, "Inserted!").send();
    		}
    		else {
    			queryMessage = (QueryMessage) theMessage;
				SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		    	queryBuilder.setTables(DynamoDBHelper.TABLE_NAME);
				Cursor reply = queryBuilder.query(readdb, new String[] {"key","value"}, "key='"+queryMessage.selection+"' AND replica='"+queryMessage.replica+"'", null, null, null, null);
				/*if (queryMessage.sourNode == queryMessage.destNode)
					cursorList.add(queryMessage.cursorNo, reply);
				else {*/
				if (reply.getCount() > 0) {
					cursorList.add(queryMessage.cursorNo, reply);
					timers.add(queryMessage.sequenceNo, false);
				}
				else {
					cursorList.add(queryMessage.cursorNo, null);
					timers.add(queryMessage.sequenceNo, false);
    			}
				new QueryMessage(Integer.toString(queryMessage.destNode),Integer.toString(queryMessage.sourNode), queryMessage.cursorNo, queryMessage.sequenceNo,null,null,reply,"Query!").send();
    		}
    	}
    	recovering = false;
    }
    /*
     * Create Insert Message during recovery
     */
    public synchronized void addInsert(String sourceNode, String destinationNode, int sequenceNo, HashMap<String,String> values, String returnValue) {
    	bufferedMessages.add(new InsertMessage(sourceNode, destinationNode, sequenceNo, values, null));
    }
    /*
     * Create Query Message during recovery
     */
    public synchronized void addQuery(String sourNode, String destinationNode, int cursorNo, int sequenceNo, String selection, String replica, Cursor returnCursor, String returnValue) {
    	bufferedMessages.add(new QueryMessage(sourNode,destinationNode,cursorNo,sequenceNo,selection,replica,returnCursor,returnValue));
    }
    /*
     * Insert into Coordinator for a particular KeyHash value
     */
	public synchronized void sentInsertCoordinator(String keyHash, ContentValues values) {

		LinkedHashMap<String,String[]> nodeList = dynamoNode.allPeers.getHashedNodeList();
		Iterator<Entry<String, String[]>> nodeEntry = nodeList.entrySet().iterator();
		int sequenceNo;
		
		while (nodeEntry.hasNext()) {
			Map.Entry<String,String[]> quorum = (Map.Entry<String,String[]>) nodeEntry.next();
			if (keyHash.compareTo(quorum.getKey()) < 0) {
				System.out.println("keyhash vs peerHash = "+ keyHash.compareTo(quorum.getKey())+" Key:"+values.getAsString("key"));
				values.put("replica", quorum.getValue()[0]);
				sequenceNo = timers.size();
				timers.add(true); 
				System.out.println("sending to original " + quorum.getValue()[0] + " SeqNo:"+sequenceNo);
				insertCoordOptimizer(dynamoNode.nodeID, quorum.getValue()[0], sequenceNo, Marshall.contentValueToHashMap(values), null);
				sequenceNo = timers.size();
				timers.add(true);
				System.out.println("sending to 1st replica " + quorum.getValue()[1] + " SeqNo:"+sequenceNo);
				insertCoordOptimizer(dynamoNode.nodeID, quorum.getValue()[1], sequenceNo, Marshall.contentValueToHashMap(values), null);
				sequenceNo = timers.size();
				timers.add(true);
				System.out.println("sending to 2nd replica " + quorum.getValue()[2] + " SeqNo:"+sequenceNo);
				insertCoordOptimizer(dynamoNode.nodeID, quorum.getValue()[2], sequenceNo, Marshall.contentValueToHashMap(values), null);
				return;
			}
		} 
		values.put("replica", "5562");
		sequenceNo = timers.size();
		timers.add(true);
		insertCoordOptimizer(dynamoNode.nodeID, "5562", sequenceNo, Marshall.contentValueToHashMap(values), null);
		sequenceNo = timers.size();
		timers.add(true);
		insertCoordOptimizer(dynamoNode.nodeID, "5556", sequenceNo, Marshall.contentValueToHashMap(values), null);
		sequenceNo = timers.size();
		timers.add(true);
		insertCoordOptimizer(dynamoNode.nodeID, "5554", sequenceNo, Marshall.contentValueToHashMap(values), null);
	}
	public synchronized void insertCoordOptimizer(String sourceNode, String destinationNode, int sequenceNo, HashMap<String,String> values, String returnValue) {
			if (destinationNode.equals(dynamoNode.nodeID)) {
				if (recovering) {
					Log.e("In Recovery Insert", "SeqNo:"+sequenceNo);
					addInsert(sourceNode,destinationNode,sequenceNo,values,returnValue);
					waitForInsert(sequenceNo);
				}
				else {
					String key = "Bogus";
					for(String keyed: values.keySet()) {
						key = keyed;
						break;
					}
			    	SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
			    	queryBuilder.setTables(DynamoDBHelper.TABLE_NAME);
					queryBuilder.appendWhere("key="+"'"+key+"'");
					Cursor cursor = queryBuilder.query(readdb, null, null, null, null, null, null);
					if (cursor.getCount() > 0) {
						Log.v("Delete Key:", key);
						//writedb.delete(DynamoDBHelper.TABLE_NAME,"key="+"'"+key+"'", null);
						//writedb.insert(DynamoDBHelper.TABLE_NAME, null, Marshall.hashMapToContentValue(values));
						writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE key = '"+key+"'");
						writedb.replace(DynamoDBHelper.TABLE_NAME, null, Marshall.hashMapToContentValue(values));
						Log.v("local insert", values.toString());
					}
					else {
						writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE key = '"+key+"'");
						writedb.replace(DynamoDBHelper.TABLE_NAME, null, Marshall.hashMapToContentValue(values));
						Log.v("local insert", values.toString());
					}
				}
		}
		else {
			new InsertMessage(sourceNode, destinationNode, sequenceNo, values, null).send();
			waitForInsert(sequenceNo);
		}
	}
	/*
	 * Delete from Coordinator for particular key value
	 */
	public synchronized void sendDeleteCoordinator(String keyHash, String selection) {
		LinkedHashMap<String,String[]> nodeList = dynamoNode.allPeers.getHashedNodeList();
		Iterator<Entry<String, String[]>> nodeEntry = nodeList.entrySet().iterator();
		while (nodeEntry.hasNext()) {
			Map.Entry<String,String[]> quorum = (Map.Entry<String,String[]>) nodeEntry.next();
			if (keyHash.compareTo(quorum.getKey()) < 0) {
				new DeleteMessage(dynamoNode.nodeID, quorum.getValue()[0], selection, quorum.getValue()[0], null);
				waitForDelete();
				new DeleteMessage(dynamoNode.nodeID, quorum.getValue()[1], selection, quorum.getValue()[0], null);
				waitForDelete();
				new DeleteMessage(dynamoNode.nodeID, quorum.getValue()[2], selection, quorum.getValue()[0], null);
				waitForDelete();
				return;
			}
		} 
		new DeleteMessage(dynamoNode.nodeID, "5562", selection,"5562", null);
		waitForDelete();
		new DeleteMessage(dynamoNode.nodeID, "5556", selection,"5562", null);
		waitForDelete();
		new DeleteMessage(dynamoNode.nodeID, "5554", selection,"5562", null);
		waitForDelete();
	}
	/*
	 * Query from Coordinator a particular key
	 */
	public synchronized void sentQueryCoordinator(String keyHash,String selection,int cursorNo) {
		int sequenceNo;
		LinkedHashMap<String,String[]> nodeList = dynamoNode.allPeers.getHashedNodeList();
		Iterator<Entry<String, String[]>> nodeEntry = nodeList.entrySet().iterator();
		while (nodeEntry.hasNext()) {
			Map.Entry<String,String[]> quorum = (Map.Entry<String,String[]>) nodeEntry.next();
			if (keyHash.compareTo(quorum.getKey()) < 0) {
				sequenceNo = timers.size();
				timers.add(true);
				System.out.println("Query for Node: "+quorum.getValue()[0]+" SeqNo:"+sequenceNo);
				queryCoordOptimizer(dynamoNode.nodeID, quorum.getValue()[0], cursorNo, sequenceNo, selection, quorum.getValue()[0], null, null);
				if (cursorList.get(cursorNo) == null) {
					sequenceNo = timers.size();
					timers.add(true);
					System.out.println("Query for 1st replica Node: "+quorum.getValue()[1]+" SeqNo:"+sequenceNo);
					queryCoordOptimizer(dynamoNode.nodeID, quorum.getValue()[1], cursorNo, sequenceNo, selection, quorum.getValue()[0], null, null);
				}
				if (cursorList.get(cursorNo) == null) {
					sequenceNo = timers.size();
					timers.add(true);
					System.out.println("Query for 2nd replica Node: "+quorum.getValue()[2]+" SeqNo:"+sequenceNo);
					queryCoordOptimizer(dynamoNode.nodeID, quorum.getValue()[2], cursorNo, sequenceNo, selection, quorum.getValue()[0], null, null);
				}
				return;
			}
		} 
		sequenceNo = timers.size();
		timers.add(true);
		System.out.println("Query for Node: 5562 SeqNo:"+sequenceNo);
		queryCoordOptimizer(dynamoNode.nodeID, "5562", cursorNo, sequenceNo, selection, "5562", null, null);
		if (cursorList.get(cursorNo) == null) {
			sequenceNo = timers.size();
			timers.add(true);
			System.out.println("Query for Node: 5556 SeqNo:"+sequenceNo);
			queryCoordOptimizer(dynamoNode.nodeID, "5556", cursorNo, sequenceNo, selection, "5562", null, null);
		}
		if (cursorList.get(cursorNo) == null) {
			sequenceNo = timers.size();
			timers.add(true);
			System.out.println("Query for Node: 5554 SeqNo:"+sequenceNo);
			queryCoordOptimizer(dynamoNode.nodeID, "5554", cursorNo, sequenceNo, selection, "5562", null, null);
		}
	}
	public synchronized void queryCoordOptimizer(String sourNode, String destinationNode, int cursorNo, int sequenceNo, String selection, String replica, Cursor returnCursor, String returnValue) {
		if (destinationNode.equals(dynamoNode.nodeID)) {
			if (recovering) {
				addQuery(sourNode,destinationNode,cursorNo,sequenceNo,selection,replica,returnCursor,returnValue);
				waitForQuery(sequenceNo);
			}
			else {
				SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		    	queryBuilder.setTables(DynamoDBHelper.TABLE_NAME);
				Cursor reply = queryBuilder.query(readdb, new String[] {"key","value"}, "key='"+selection+"' AND replica='"+replica+"'", null, null, null, null);
				cursorList.add(cursorNo, reply);
			}
		}
		else {
			new QueryMessage(sourNode,destinationNode,cursorNo,sequenceNo,selection,replica,returnCursor,returnValue).send();
			waitForQuery(sequenceNo);
		}
	}
	/*
	 * Linearizibity - Blocking until insert is done
	 */
	public void waitForInsert(int sequenceNo){
		try {
			int i = 0;
			while (timers.get(sequenceNo) && i<5) {
				Thread.sleep(100);
				i++;
			} 
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		timers.add(sequenceNo, true);
	}
	/*
	 * Linearizibility - Blocking until delete is done
	 */
	public void waitForDelete() {
		try {
			int i = 0;
			while (noDeleteResponse && i<5) {
				Thread.sleep(100);
				i++;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		noDeleteResponse = true;
	}
	/*
	 * Linearizibity - Blocking until query is done
	 */
	public void waitForQuery(int sequenceNo) {
		try {
			int i = 0;
			while (timers.get(sequenceNo) && i<5) {
				Thread.sleep(100);
				i++;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		timers.add(sequenceNo, true);
	}
	public Cursor recoverData(String replica) {
    	SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
    	queryBuilder.setTables(DynamoDBHelper.TABLE_NAME);
    	queryBuilder.appendWhere("replica='"+replica+"'");
    	Cursor cursor = queryBuilder.query(readdb, null, null, null, null, null, null);
    	return cursor;
	}
	public void addLostData(HashMap<String,String> contentMap, String replica){
		ContentValues row;
		for (String key : contentMap.keySet()) {
			row = new ContentValues();
			row.put("key", key);
			row.put("value", contentMap.get(key));
			row.put("replica", replica);
			writedb.execSQL("DELETE FROM "+ DynamoDBHelper.TABLE_NAME + " WHERE key = '"+key+"'");
			writedb.replace(DynamoDBHelper.TABLE_NAME, null, row);
		}
	}
	public void restart() {
		serverTask = new ServerTask(SERVER_PORT);
	}
	/*
	 * ServerTask starts in a Thread on ContentProvider creation
	 */
	private class ServerTask extends Thread {
		private int serverPort;
		
		ServerTask(int serverPort) {
			this.serverPort = serverPort;
			this.start();
		}

		public void run() {
			try {
				ServerSocket serverSocket = new ServerSocket(serverPort);
				while(true) {
					Socket sock = serverSocket.accept();
					InputStream is = sock.getInputStream();
					ObjectInputStream ois = new ObjectInputStream(is);
					Object gotMessage = ois.readObject();
					ProcessMessage message = new ProcessMessage(gotMessage);
					//run1(gotMessage);
				}
			} catch (EOFException e) {
				Log.e(TAG,"EOF Exception Server Socket");
				restart();
			} catch (IOException e) {
				Log.e(TAG,"IO ERROR SERVER SOCKET");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
	} // end of ServerTask
	/*
	 * Message Processing Class
	 */
	private class ProcessMessage extends Thread{
		Object message;
		
		ProcessMessage(Object gotMessage) {
			this.message = gotMessage;
			this.start();
		}
		public void run() {
			if (message instanceof InsertMessage) {
				processInsert((InsertMessage) message);
			}
			else if (message instanceof DeleteMessage) {
				processDelete((DeleteMessage) message);
			}
			else if (message instanceof QueryMessage) {
				processQuery((QueryMessage) message);
			}
			else if (message instanceof MyBackup) {
				processMyBackup((MyBackup) message);
			}

		}
		
		void processInsert(InsertMessage message) {
			System.out.println("Is recovery? "+recovering);
			if (recovering && message.returnValue == null) {
				Log.e("In Recovery Insert", "SeqNo:"+message.sequenceNo);
				bufferedMessages.add(message);
			}
			else if (message.returnValue == null) {
				System.out.println("Foreign InsertMessage Node: "+message.sourNode+" SeqNo:"+message.sequenceNo);
				insertOperator(MESSAGES_URI,Marshall.hashMapToContentValue(message.values));
				new InsertMessage(String.valueOf(message.destNode), String.valueOf(message.sourNode), message.sequenceNo, null, "Inserted!").send();
			}
			else if (message.returnValue.equals("Insert Failed")){
				System.out.println("Insert Failed For SeqNo:"+message.sequenceNo);
				timers.add(message.sequenceNo, false);
			}
			else {
				System.out.println("Returned InsertMessage!"+" SeqNo:"+message.sequenceNo);
				timers.add(message.sequenceNo, false);
			}
		}
		void processDelete(DeleteMessage message) {
			if (message.returnValue == null) {
				deleteOperator(MESSAGES_URI, message.selection, message.replica);
				new DeleteMessage(String.valueOf(message.destNode), String.valueOf(message.sourNode), null, null, "Deleted!");
			}
			else if (message.returnValue.equals("Delete Failed")) {
				noDeleteResponse = false;
			}
			else {
				noDeleteResponse = false;
			}
		}
		void processQuery(QueryMessage message) {
			System.out.println("Is recovery? "+recovering);
			if (recovering && message.returnValue == null) {
				bufferedMessages.add(message);
			}
			else if (message.returnValue == null) {
				System.out.println("Foreign QueryMessage Node: "+message.sourNode+" SeqNo:"+message.sequenceNo);
				Cursor reply = queryOperator(MESSAGES_URI, message.selection, message.replica);
				//Cursor finalReply = getLatestCursor(Marshall.hashMaptoCursor(message.returnCursor),reply);
				new QueryMessage(String.valueOf(message.destNode),String.valueOf(message.sourNode), message.cursorNo, message.sequenceNo, null, null, reply, "Queried!").send();
			}
			else if (message.returnValue.equals("Query Failed") || message.returnCursor.isEmpty()) {
				System.out.println("Query Failed For SeqNo:"+message.sequenceNo);
				cursorList.add(message.cursorNo,null);
				timers.add(message.sequenceNo, false);
			}
			else {
				System.out.println("Returned QueryMessage!"+" SeqNo:"+message.sequenceNo);
				cursorList.add(message.cursorNo, Marshall.hashMaptoCursor(message.returnCursor));
				timers.add(message.sequenceNo, false);
			}
		}
		void processMyBackup(MyBackup message) {
			if (message.returnValue == null) {
				Log.e("Processing Backup for", message.backupOf);
		    	Cursor cursor = recoverData(message.backupOf);
		    	new MyBackup(String.valueOf(message.destNode),String.valueOf(message.sourNode),message.backupOf,Marshall.cursorToHashMap(cursor),"Backup!");
			}
			else {
				Log.e("Got MyBackup from ", String.valueOf(message.sourNode));
				addLostData(message.returnCursor, message.backupOf);
			}
		}
	}//end of Message Processing
	
	/*
	 * Recover the Node after a failure
	 */
	class RecoverNode extends Thread{
		
		RecoverNode() {
			this.start();
		}
		
		public void run(){
			//cleanTheDataBase();
			for (String quorumPeer: dynamoNode.quorumPeers) {
				Log.e("Asking For my Backup from ",quorumPeer);
				new MyBackup(dynamoNode.nodeID,quorumPeer,dynamoNode.nodeID,null,null);
			}
			for (String recoveryPeer: dynamoNode.replicaPeers) {
				Log.e("Asking for their Backup ", recoveryPeer);
				new MyBackup(dynamoNode.nodeID,recoveryPeer,recoveryPeer,null,null);
			}
			computeBuffer();
		}
		
	} // end of recovery class
	
} // end of content provider
