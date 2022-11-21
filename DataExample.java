import java.util.ArrayList;
import java.util.List;

class DataExample implements Data {
    // Fields
    private int dataID;
    private List<Integer> data;
    // Methods
    DataExample() {dataID=0;}
    DataExample(List<Integer> data, int dataID) { this.data = data; this.dataID = dataID; }
    @Override
    public int getDataId() { return dataID; }
    @Override
    public int getSize() { return data.size(); }
    @Override
    public int getValue(int idx) { return data.get(idx); }
}