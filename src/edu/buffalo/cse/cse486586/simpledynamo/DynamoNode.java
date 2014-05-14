package edu.buffalo.cse.cse486586.simpledynamo;

public class DynamoNode extends Thread{
	String nodeID;
	String nodeIDHash;
	String predecessor;
	String predecessorHash;
	String[] quorumPeers;
	String[] replicaPeers;
	public static HashedNodeList allPeers; 

	
	public DynamoNode(String nodeID) {
		this.nodeID = nodeID;
		this.allPeers = new HashedNodeList();
		this.run();
	}
	
	/*
	 *	As soon as DynamoNode comes up it will get Objects that belong to it. 
	 */
	public void run(){
		nodeIDHash = SHA1.genHash(nodeID);
		int myNode = Integer.parseInt(nodeID);
		switch(myNode) 
		{
			case 5562:  quorumPeers = new String[] {"5556","5554"};
						replicaPeers = new String[] {"5558", "5560"};
					    break;
			case 5556:  quorumPeers = new String[] {"5554","5558"};
						replicaPeers = new String[] {"5562","5560"};
					    break;
			case 5554:  quorumPeers = new String[] {"5558","5560"};
						replicaPeers = new String[] {"5556","5562"};
						break;
			case 5558: 	quorumPeers = new String[] {"5560","5562"};
						replicaPeers = new String[] {"5554","5556"};
						break;
			case 5560:  quorumPeers = new String[] {"5562","5556"};
						replicaPeers = new String[] {"5558","5554"};
						break;
		}
	}
}
