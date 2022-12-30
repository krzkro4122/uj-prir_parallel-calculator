import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;



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

        this.deltas.addAll(deltas);

       {
            String output = "";

            output += "\t[" + Thread.currentThread().getId() + "] Got deltas: \n";

            for (Delta delta : deltas)
                output += "{dataID: " + delta.getDataID() +
                    ", deltaIndex: " + delta.getIdx() +
                    ", deltaValue: " + delta.getDelta() + " " + "length: " + deltas.size() + "}\n";

            System.out.println(output);
        }
    }
}


class ParallelCalculator implements DeltaParallelCalculator {

    // Fields
    private int threads;
    private AtomicInteger lowestId = new AtomicInteger(0);

    private DeltaReceiver deltaReceiver;
    private ExecutorService executorService;
    private ConcurrentSkipListMap<Integer, Data> datas = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Integer, DeltaProxy> proxies = new ConcurrentSkipListMap<>();

    // Private Classes
    private class Task implements Runnable {

        private int dataIdx;
        final private int thread;
        final private Data data1;
        final private Data data2;

        private List<Delta> deltas;

        Task (int thread, int dataIdx, Data data1, Data data2) {

            this.data1 = data1;
            this.data2 = data2;
            this.thread = thread;
            this.dataIdx = dataIdx;
            this.deltas = new ArrayList<>();
        }

        public int getDataId() {
            return dataIdx;
        }

        // This does all the dirty work AKA computations
        @Override
        public void run() {

            // Compute and memoize the deltas
            for (int i = thread; i < data1.getSize(); i += threads) {

                int data1Val = data1.getValue(i);
                int data2Val = data2.getValue(i);

                if ( data1Val != data2Val ) {

                    deltas.add(new Delta(dataIdx, i, data2Val - data1Val));

                }

            }
            synchronized (proxies) {
                proxies.get(dataIdx).addAll(deltas);
            }
        }

    }

    private class DeltaProxy {

        private List<Delta> deltas;
        private AtomicInteger counter;

        DeltaProxy(int dataId) {

            counter = new AtomicInteger(0);
            deltas = new ArrayList<>();

        }

        public void addAll(List<Delta> incomingDeltas) {

            synchronized (proxies) {

                deltas.addAll(incomingDeltas);
                counter.set(counter.get() + 1);

                if (counter.get() != threads) {
                    return;
                }
            }

            for (int id : proxies.keySet()) {

                synchronized (proxies) {

                    if (lowestId.get() == id) {

                        if (proxies.get(id).counter.get() != threads) {
                            return;
                        }

                        deltaReceiver.accept(proxies.get(id).deltas);
                        lowestId.set(lowestId.get() + 1);

                    }
                }
            }
        }
    }

    // Methods
    @Override
    public void setThreadsNumber(int threads) {

        this.threads = threads;

        executorService = new ThreadPoolExecutor(
            this.threads,
            this.threads,
            100L,
            TimeUnit.MILLISECONDS,
            new PriorityBlockingQueue<>(11, (firstTask, secondTask) -> {
                return Comparator.comparingInt(Task::getDataId).compare( (Task) firstTask, (Task) secondTask );
            })
        );
    }

    @Override
    public void setDeltaReceiver(DeltaReceiver receiver) {
        deltaReceiver = receiver;
    }

    @Override
    public void addData(Data data) {

        // Add data to the list
        datas.put(data.getDataId(), data);
        System.out.println(datas);

        // Act upon a Data pair only if the neighbouring Data IDs are uniform
        for (int i : datas.keySet()) {

            Data dataOne = null;
            Data dataTwo = null;

            dataOne = datas.get(i);
            dataTwo = datas.get(i + 1);

            if (dataTwo == null) {
                continue;
            }

            int dataId = dataOne.getDataId();

            if (proxies.get(dataId) != null) {
                continue;
            }

            // String output = "[";
            // for (int m = 0; m < dataOne.getSize(); m++) {
            //     output += dataOne.getValue(m);
            // } output += "  " + dataOne.getDataId() + " VS. " + dataTwo.getDataId() +  "  ";
            // for (int m = 0; m < dataTwo.getSize(); m++) {
            //     output += dataTwo.getValue(m);
            // } System.out.println(output + "]");

            if (dataId + 1 != datas.get(i + 1).getDataId()) {
                continue;
            }

            proxies.putIfAbsent(dataId, new DeltaProxy(dataId));

            for (int k = 0; k < this.threads; k++) {

                executorService.execute( new Task(k, dataId, datas.get(i), datas.get(i + 1)) );

            }
        }
    }

    // Main
    public static void main(String[] args) {

        ParallelCalculator pc = new ParallelCalculator();
        pc.setDeltaReceiver(new DeltaReceiverExample());
        pc.setThreadsNumber(5);

        // TEST
        List<Integer> indeces = List.of(5, 3, 4, 0, 1, 2);

        // Mock data
        int data_size = indeces.size();
        for (int i = 0; i < data_size; i++) {

            List<Integer> list = new ArrayList<>();

            for (int j = 0; j < data_size; j++)
                list.add(i);

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
