
public interface DeltaParallelCalculator {
	public void setThreadsNumber(int threads);


	public void setDeltaReceiver(DeltaReceiver receiver);

	public void addData(Data data);
}