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

import android.util.Log;

public class MyBackup extends Thread implements Serializable {

	int sourNode;
	int destNode;
	String backupOf;
	HashMap<String,String> returnCursor;
	String returnValue;
	private static final String TAG = MyBackup.class.getSimpleName();
	
	public MyBackup(String sourNode, String destNode, String backupOf, HashMap<String,String> returnCursor, String returnValue) {
		this.sourNode = Integer.parseInt(sourNode);
		this.destNode = Integer.parseInt(destNode);
		this.backupOf = backupOf;
		this.returnCursor = returnCursor;
		this.returnValue = returnValue;
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
            socket.close();
        } catch (SocketTimeoutException e) {
        	Log.e(TAG, "Socket Timeout Exception: Node " + destNode + " is down!");
        } catch (EOFException e) {
        	Log.e(TAG, "EOFException");
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException Recovery Failed");
        }
	}	
}
