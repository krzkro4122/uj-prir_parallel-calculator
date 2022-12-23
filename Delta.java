import java.util.Objects;

public class Delta {
	private final int dataID;
	private final int idx;
	private final int delta;

	public Delta(int dataID, int idx, int delta) {
		this.dataID = dataID;
		this.idx = idx;
		this.delta = delta;
	}
	public int getDataID() {
		return dataID;
	}
	public int getIdx() {
		return idx;
	}

	public int getDelta() {
		return delta;
	}

	@Override
	public int hashCode() {
		return Objects.hash(dataID, delta, idx);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Delta other = (Delta) obj;
		return dataID == other.dataID && delta == other.delta && idx == other.idx;
	}
}