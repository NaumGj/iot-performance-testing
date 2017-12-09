package top3;

public class MostFreqDrivenTop3 {
	
	private static MostFreqDrivenMsg[] mostFreqDriven = {new MostFreqDrivenMsg("", 0L), new MostFreqDrivenMsg("", 0L), new MostFreqDrivenMsg("", 0L)};

	public static MostFreqDrivenMsg[] getMostFreqDriven() {
		return mostFreqDriven;
	}

	public static void setMostFreqDriven(MostFreqDrivenMsg[] mostFreqDriven) {
		MostFreqDrivenTop3.mostFreqDriven = mostFreqDriven;
	}
	
	public static String getString() {
		return mostFreqDriven[0].getMostFreqDriven() + "," + String.valueOf(mostFreqDriven[0].getMostFreqDrivenCnt()) + ";" +
				mostFreqDriven[1].getMostFreqDriven() + "," + String.valueOf(mostFreqDriven[1].getMostFreqDrivenCnt()) + ";" +
				mostFreqDriven[2].getMostFreqDriven() + "," + String.valueOf(mostFreqDriven[2].getMostFreqDrivenCnt()) + ";";
	}

	public static void handleEntry(String key, Long value) {
		if (key.equals(mostFreqDriven[0].getMostFreqDriven())) {
			if (value > mostFreqDriven[0].getMostFreqDrivenCnt()) {
				mostFreqDriven[0].setMostFreqDrivenCnt(value);
			}
			return;
		}

		if (key.equals(mostFreqDriven[1].getMostFreqDriven())) {
			if (value > mostFreqDriven[1].getMostFreqDrivenCnt()) {
				mostFreqDriven[1].setMostFreqDrivenCnt(value);
			}
			return;
		}
		
		if (key.equals(mostFreqDriven[2].getMostFreqDriven())) {
			if (value > mostFreqDriven[2].getMostFreqDrivenCnt()) {
				mostFreqDriven[2].setMostFreqDrivenCnt(value);
			}
			return;
		}
		
		if (value > mostFreqDriven[0].getMostFreqDrivenCnt()) {
			mostFreqDriven[2].setMostFreqDriven(mostFreqDriven[1].getMostFreqDriven());
			mostFreqDriven[2].setMostFreqDrivenCnt(mostFreqDriven[1].getMostFreqDrivenCnt());
			
			mostFreqDriven[1].setMostFreqDriven(mostFreqDriven[0].getMostFreqDriven());
			mostFreqDriven[1].setMostFreqDrivenCnt(mostFreqDriven[0].getMostFreqDrivenCnt());
			
			mostFreqDriven[0].setMostFreqDriven(key);
			mostFreqDriven[0].setMostFreqDrivenCnt(value);
			
			return;
		}
		
		if (value > mostFreqDriven[1].getMostFreqDrivenCnt()) {
			mostFreqDriven[2].setMostFreqDriven(mostFreqDriven[1].getMostFreqDriven());
			mostFreqDriven[2].setMostFreqDrivenCnt(mostFreqDriven[1].getMostFreqDrivenCnt());
			
			mostFreqDriven[1].setMostFreqDriven(key);
			mostFreqDriven[1].setMostFreqDrivenCnt(value);
			
			return;
		}
		
		if (value > mostFreqDriven[2].getMostFreqDrivenCnt()) {
			mostFreqDriven[2].setMostFreqDriven(key);
			mostFreqDriven[2].setMostFreqDrivenCnt(value);
			
			return;
		}
	}

}
