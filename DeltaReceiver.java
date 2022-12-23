import java.util.List;


public interface DeltaReceiver {
	public void accept( List<Delta> deltas );
}