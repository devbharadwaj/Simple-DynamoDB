package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.LinkedHashMap;

public class HashedNodeList {
	
	public LinkedHashMap<String,String[]> quorumMap;
	
	public HashedNodeList() {
		quorumMap = new LinkedHashMap<String,String[]>();
		quorumMap.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1", new String[] {"5562","5556","5554"});
		quorumMap.put("208f7f72b198dadd244e61801abe1ec3a4857bc9", new String[] {"5556","5554","5558"});
		quorumMap.put("33d6357cfaaf0f72991b0ecd8c56da066613c089", new String[] {"5554","5558","5560"});
		quorumMap.put("abf0fd8db03e5ecb199a9b82929e9db79b909643", new String[] {"5558","5560","5562"});
		quorumMap.put("c25ddd596aa7c81fa12378fa725f706d54325d12", new String[] {"5560","5562","5556"});
	}
	
	public LinkedHashMap<String,String[]> getHashedNodeList() {
		return quorumMap;
	}
}
