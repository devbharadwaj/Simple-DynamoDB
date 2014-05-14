package edu.buffalo.cse.cse486586.simpledynamo;

public class CircularLinkedList {

	Node head;
	
	public CircularLinkedList() {
		this.head = new Node();
		Node FifthNode = new Node("5560", "c25ddd596aa7c81fa12378fa725f706d54325d12", head);
		Node FourthNode = new Node("5558", "abf0fd8db03e5ecb199a9b82929e9db79b909643", FifthNode);
		Node ThirdNode = new Node("5554", "33d6357cfaaf0f72991b0ecd8c56da066613c089", FourthNode);
		Node SecondNode = new Node("5556", "208f7f72b198dadd244e61801abe1ec3a4857bc9", ThirdNode);
		head.peer = "5562";
		head.peerHash = "177ccecaec32c54b82d5aaafc18a2dadb753e3b1";
		head.next = SecondNode;
	}
	
	public Node getNext(Node currentNode) {
		return currentNode.next;
	}
	
	class Node{
		public String peer;
		public String peerHash;
		public Node next;
		
		Node() {
			
		}
		
		Node(String peer, String peerHash, Node next) {
			this.peer = peer;
			this.peerHash = peerHash;
			this.next = next;
		}
	}

}
