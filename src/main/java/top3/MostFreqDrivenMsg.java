package top3;

public class MostFreqDrivenMsg {

	private String mostFreqDriven;
	private Long mostFreqDrivenCnt;
	
	public MostFreqDrivenMsg(String mostFreqDriven, Long mostFreqDrivenCnt) {
		this.mostFreqDriven = mostFreqDriven;
		this.mostFreqDrivenCnt = mostFreqDrivenCnt;
	}

	public String getMostFreqDriven() {
		return mostFreqDriven;
	}

	public void setMostFreqDriven(String mostFreqDriven) {
		this.mostFreqDriven = mostFreqDriven;
	}

	public Long getMostFreqDrivenCnt() {
		return mostFreqDrivenCnt;
	}

	public void setMostFreqDrivenCnt(Long mostFreqDrivenCnt) {
		this.mostFreqDrivenCnt = mostFreqDrivenCnt;
	}
	
}
