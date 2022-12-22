import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;

import java.util.Comparator;
import java.util.Vector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


class DataExample implements Data {
    // Fields
    private int dataID;
    private List<Integer> data;
    // Methods
    DataExample() { dataID=0; }
    DataExample(List<Integer> data, int dataID) { this.data = data; this.dataID = dataID; }
    @Override
    public int getDataId() { return dataID; }
    @Override
    public int getSize() { return data.size(); }
    @Override
    public int getValue(int idx) { return data.get(idx); }
}


class DeltaReceiverExample implements DeltaReceiver {

    //Fields
    private final List<Delta> deltas;

    // Methods
    DeltaReceiverExample() {
        deltas = new ArrayList<>();
    }

    @Override
    public void accept(List<Delta> deltas) {

        for (Delta delta : deltas) {
            this.deltas.add(delta);
        }

       {
            String output = "";

            output += "\t[" + Thread.currentThread().getId() + "] GOTTEN DELTAS:";

            for (Delta delta : deltas)
                output += "\n\t\t{deltaID: " + delta.getDataID() +
                    ", deltaIndex: " + delta.getIdx() +
                    ", deltaValue: " + delta.getDelta() + "" + "}";

            System.out.println(output + "\n\t\tList<Delta>'s length: " + deltas.size());
        }

    }
}


class ParallelCalculator implements DeltaParallelCalculator {

    // Fields
    private int threads;

    private Set<Integer> sentDataIds;
    private DeltaReceiver deltaReceiver;
    private ExecutorService executorService;
    private ConcurrentSkipListMap<Integer, Data> datas;

    // Private Classes
    private class Task implements Callable<List<Delta>> {

        private int id;
        private int dataId;
        private Data data1;
        private Data data2;

        Task (int id, int dataId, Data data1, Data data2) {
            this.id = id;
            this.dataId = dataId;
            this.data1 = data1;
            this.data2 = data2;
        }

        public synchronized int getId() {
            return id;
        }

        public synchronized int getDataId() {
            return dataId;
        }

        // This does all the dirty work AKA computations
        @Override
        public synchronized List<Delta> call() {

            List<Delta> deltas = new ArrayList<>();

            // Both the same size
            for (int i = id; i < data1.getSize(); i += threads) {

                if ( data1.getValue(i) != data2.getValue(i) ) {
                    deltas.add(new Delta(data1.getDataId(), i, data1.getValue(i) - data2.getValue(i)));
                }
            }

            return deltas;
        }
    }

    private class DeltaProxy {

        private Vector<Delta> deltas;
        private Vector<Integer> dataIds;

        DeltaProxy() {
            this.deltas = new Vector<>();
            this.dataIds = new Vector<>();
        }

        public synchronized void addDeltaList(List<Delta> deltas) {
            this.deltas.addAll(deltas);
            this.dataIds.add(deltas.get(0).getDataID());

            if (dataIds.size() == threads) {
                deltaReceiver.accept(deltas);
            }
        }
    }

    // Methods
    @Override
    public synchronized void setThreadsNumber(int threads) {

        this.sentDataIds = ConcurrentHashMap.newKeySet();
        this.datas = new ConcurrentSkipListMap<>();

        this.threads = threads;

        executorService = new ThreadPoolExecutor( 0, this.threads, 0L, TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(this.threads * 2, (firstTask, secondTask) -> {
                return Comparator.comparingInt(Task::getId).compare( (Task) firstTask, (Task) secondTask );
            })
        );
    }

    @Override
    public synchronized void setDeltaReceiver(DeltaReceiver receiver) {
        deltaReceiver = receiver;
    }

    @Override
    public synchronized void addData(Data data) {

        // Add data to the list
        datas.put(data.getDataId(), data);

        if (datas.size() < 2) {
            return;
        }

        // Act upon a Data pair only if the neighbouring Data IDs are uniform
        for (int i : datas.keySet()) {

            System.out.println(datas);

            if ( this.sentDataIds.contains(datas.get(i).getDataId()) )
                continue;

            if ( ! datas.containsKey(i + 1) )
                return;

            if (datas.get(i).getDataId() + 1 == datas.get(i + 1).getDataId()) {

                DeltaProxy deltaProxy = new DeltaProxy();
                this.sentDataIds.add(datas.get(i).getDataId());

                for (int k = 0; k < this.threads; k++) {

                    Future<List<Delta>> futureDeltas = executorService.submit( new Task(k, datas.get(i).getDataId(), datas.get(i), datas.get(i + 1)) );
                    List<Delta> deltas = null;

                    try {
                        deltas = futureDeltas.get();
                        deltaProxy.addDeltaList(deltas);
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    // Main
    public static void main(String[] args) {

        ParallelCalculator pc = new ParallelCalculator();
        pc.setDeltaReceiver(new DeltaReceiverExample());
        pc.setThreadsNumber(5);

        // TEST
        List<Integer> indeces = List.of(5, 4, 3, 2, 1, 0);

        // Mock data
        int data_size = indeces.size();
        for (int i = 0; i < data_size; i++) {

            List<Integer> list = new ArrayList<>();

            for (int j = 0; j < data_size; j++)
                list.add(j + i);

            Data data = new DataExample(list, indeces.get(i));
            pc.addData(data);

            // Pause!
            if (i == data_size / 2) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    System.out.println(ie.getMessage());
                }
            }
        }
        // Cleanup
        pc.executorService.shutdown();
    }
}
