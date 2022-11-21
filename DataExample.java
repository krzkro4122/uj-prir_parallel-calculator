class DataExample implements Data {
    // Fields
    private int dataID;
    private int[] data = new int[8];
    // Methods
    DataExample() {data = (new int[] {1, 2, 3, 4, 5, 6, 7, 8}).clone(); dataID=0;}
    DataExample(int[] data, int dataID) { this.data = data; this.dataID = dataID; }
    @Override
    public int getDataId() { return dataID; }
    @Override
    public int getSize() { return data.length; }
    @Override
    public int getValue(int idx) { return data[idx]; }
}