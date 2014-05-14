package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;

import android.database.Cursor;
import android.util.Log;

public class QueryMessage extends Thread implements Serializable, Messages{
	
	public String selection;
	public String replica;
	public int sourNode;
	public int destNode;
	public int cursorNo;
	public int sequenceNo;
	public HashMap<String,String> returnCursor;
	public String returnValue;
	private static final String TAG = QueryMessage.class.getSimpleName();
	
	public QueryMessage(String sourNode, String destinationNode, int cursorNo, int sequenceNo, String selection, String replica, Cursor returnCursor, String returnValue) {
		this.selection = selection;
		this.replica = replica;
		this.cursorNo = cursorNo;
		this.sequenceNo = sequenceNo;
		this.sourNode = Integer.parseInt(sourNode);
		this.destNode = Integer.parseInt(destinationNode);
		this.returnCursor = Marshall.cursorToHashMap(returnCursor);
		this.returnValue = returnValue;
		
	}
	public void send() {
		this.start();
	}
	
	public void run() {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), destNode * 2);
            socket.setSoTimeout(100);
            /*
             * TODO: Fill in your client code that sends out a message.
             */
            OutputStream os = socket.getOutputStream();
            ObjectOutputStream msgObject = new ObjectOutputStream(os);
            msgObject.writeObject(this);
            msgObject.close();
            os.close();
            socket.close();
        } catch (SocketTimeoutException e) {
        	Log.e(TAG, "Socket Timeout Exception: Node " + destNode + " is down!");
        } catch (EOFException e) {
        	Log.e(TAG, "EOFException");
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "Client Query socket IOException");
            Log.e(TAG, "Server Socket Exception: Node " + destNode + " is down!");
            new QueryMessage(String.valueOf(this.sourNode),String.valueOf(this.sourNode),this.cursorNo,this.sequenceNo,null,null,null,"Query Failed").send();
        }
	}
}

