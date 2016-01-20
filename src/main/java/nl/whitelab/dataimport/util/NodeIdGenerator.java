package nl.whitelab.dataimport.util;

public final class NodeIdGenerator {
	private long counter;

	public NodeIdGenerator() {
		counter = 0;
	}
	
	public long currentId() {
		return counter;
	}
	
	public long nextId() {
		increment(1);
		return counter;
	}
	
	private void increment(long i) {
		counter = counter + i;
	}

}
