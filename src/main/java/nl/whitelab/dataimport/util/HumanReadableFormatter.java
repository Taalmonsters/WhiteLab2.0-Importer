package nl.whitelab.dataimport.util;

import java.sql.Timestamp;
import java.text.DecimalFormat;

public class HumanReadableFormatter {
	
	public static String getTimestamp() {
		java.util.Date date= new java.util.Date();
		return new Timestamp(date.getTime()).toString();
	}
	
	public static String humanReadableTimeElapsed(long elapsed) {
		long micro = elapsed % 1000;
		elapsed = (elapsed - micro) / 1000;
		long hrs = (elapsed - (elapsed % 3600)) / 3600;
		elapsed = elapsed - (hrs * 3600);
		long mins = (elapsed - (elapsed % 60)) / 60;
		long secs = elapsed - (mins * 60);
		return String.format("%2d:%02d:%02d.%03d", hrs, mins, secs, micro);
	}

	public static String humanReadableByteCount(float bytes, boolean si) {
//	    int unit = si ? 1000 : 1024;
//	    if (bytes < unit) return bytes + " B";
//	    int exp = (int) (Math.log(bytes) / Math.log(unit));
//	    String str = (si ? "kMGTPE" : "KMGTPE");
//	    if ((exp - 1) > -1 && str.length() - 1 >= exp - 1) {
//	    	String pre = str.charAt(exp-1) + (si ? "" : "i");
//		    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
//	    }
//	    return "UNK B";
	    
	    if(bytes <= 0) return "0.0 B";
	    final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
	    int digitGroups = (int) (Math.log10(bytes)/Math.log10(1024));
//	    if (digitGroups <= 4)
	    	return new DecimalFormat("#,##0.#").format(bytes/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
//	    else
//	    	return new DecimalFormat("#,##0.#").format(bytes/Math.pow(1024, digitGroups));
	}

}
