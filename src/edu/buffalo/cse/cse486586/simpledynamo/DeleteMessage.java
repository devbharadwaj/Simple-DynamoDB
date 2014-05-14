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

import android.util.Log;

public class DeleteMessage extends Thread implements Serializable{
	
	public String selection;
	public int sourNode;
	public int destNode;
	public String replica;
	public String returnValue;
	private static final String TAG = DeleteMessage.class.getSimpleName();
	
	public DeleteMessage( String sourceNode, String destinationNode, String selection, String replica, String returnValue) {
		this.selection = selection;
		this.sourNode = Integer.parseInt(sourceNode);
		this.destNode = Integer.parseInt(destinationNode);
		this.replica = replica;
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
            Log.e(TAG, "Client Insert socket IOException");
            Log.e(TAG, "Server Socket Exception: Node " + destNode + " is down!");
            new DeleteMessage(String.valueOf(this.sourNode),String.valueOf(this.sourNode),null,null,"Delete Failed");
        }
	}
}

