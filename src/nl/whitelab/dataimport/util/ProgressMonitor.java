package nl.whitelab.dataimport.util;

import java.util.Iterator;

import nl.whitelab.dataimport.neo4j.LinkCreator;
import nl.whitelab.dataimport.neo4j.NodeCreator;

public final class ProgressMonitor {
	private long startTime = -1;
	private long lastReport = -1;
	private boolean running = false;
	private long reportInterval = 600000;

	private long dataProcessed = 0;
	private long docsProcessed = 0;
	private long tokensProcessed = 0;
	
	private static boolean permitLogging = true;

	public ProgressMonitor() {
	}

	public ProgressMonitor(int i) {
		reportInterval = i * 60000;
	}
	
	public void start() {
		running = true;
		startTime = System.currentTimeMillis();
	}
	
	public void stop() {
		running = false;
		report();
	}
	
	public boolean isRunning() {
		return running;
	}
	
	public void update(long fileSize, int documentCount, int tokenCount) {
		dataProcessed = dataProcessed + fileSize;
		docsProcessed = docsProcessed + documentCount;
		tokensProcessed = tokensProcessed + tokenCount;
		report();
	}
	
	public void update(long fileSize, int documentCount, long tokenCount) {
		dataProcessed = dataProcessed + fileSize;
		docsProcessed = docsProcessed + documentCount;
		tokensProcessed = tokensProcessed + tokenCount;
		report();
	}

	public void report() {
		long current = System.currentTimeMillis();
		if (!running || (current - lastReport >= reportInterval)) {
			long elapsed = current - startTime;
			if (elapsed > 0) {
				String timestamp = HumanReadableFormatter.humanReadableTimeElapsed(elapsed);
				String data = HumanReadableFormatter.humanReadableByteCount(dataProcessed, true);
				float secs = (float) (elapsed / 1000.0);
				float x = dataProcessed / secs;
				String dataPerSecond = HumanReadableFormatter.humanReadableByteCount(x, false);
				String docsPerSecond = String.format("%.1f", docsProcessed / secs);
				String tokensPerSecond = String.format("%.1f", tokensProcessed / secs);
				System.out.println(timestamp+" - Processed "+tokensProcessed+" tokens ("+tokensPerSecond+" t/s), "+docsProcessed+" documents ("+docsPerSecond+" d/s), "+data+" ("+dataPerSecond+"/s)");
				lastReport = current;
			}
		}
	}
	
	public void log(String msg) {
		if (permitLogging) {
			long elapsed = System.currentTimeMillis() - startTime;
			String timestamp = HumanReadableFormatter.humanReadableTimeElapsed(elapsed);
			System.out.println(timestamp + " - " + msg);
		}
		
	}
	
	public void endReport(NodeCreator nc, LinkCreator lc) {
		long current = System.currentTimeMillis();
		long elapsed = current - startTime;
		String timestamp = HumanReadableFormatter.humanReadableTimeElapsed(elapsed);
		String data = HumanReadableFormatter.humanReadableByteCount(dataProcessed, true);
		float secs = elapsed / 1000;
		String dataPerSecond = HumanReadableFormatter.humanReadableByteCount(dataProcessed / secs, false);
		String docsPerSecond = String.format("%.1f", docsProcessed / secs);
		String tokensPerSecond = String.format("%.1f", tokensProcessed / secs);
		System.out.println(timestamp+" - Processed "+tokensProcessed+" tokens ("+tokensPerSecond+" t/s), "+docsProcessed+" documents ("+docsPerSecond+" d/s), "+data+" ("+dataPerSecond+"/s)");

		Iterator<String> labels = nc.getCountKeys();
		System.out.println(timestamp+" - Added "+nc.getNodeCount()+" nodes:");
		while (labels.hasNext()) {
			String label = labels.next();
			System.out.println(timestamp+" -\t"+label.toString()+": "+nc.getNodeCount(label));
		}

		Iterator<String> links = lc.getCountKeys();
		System.out.println("");
		System.out.println(timestamp+" - Added "+lc.getLinkCount()+" links:");
		while (links.hasNext()) {
			String label = links.next();
			System.out.println(timestamp+" -\t"+label.toString()+": "+lc.getLinkCount(label));
		}
		
	}
}
