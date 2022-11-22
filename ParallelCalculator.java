import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class ParallelCalculator implements DeltaParallelCalculator {
    // Fields
    private int threads = 0;

    private DeltaReceiver deltaReceiver;
    private ExecutorService executorService;

    protected List<Data> dataContainer = new ArrayList<Data>();
    protected List<Integer> calculatedEntries = new ArrayList<Integer>();

    // Methods
    @Override
    public void setThreadsNumber(int threads) {

        this.threads = threads;

        executorService = new ThreadPoolExecutor(
            this.threads,
            this.threads,
            0L,
            TimeUnit.SECONDS,
            new TaskQueue(this.threads * 5)
        );
    }
 
    private <T> String formatCollection(Collection<T> collection) {
        String output = new String();
        output += "[";
        for (T element : collection) {
            output += element + ", ";
        }
        output += "], length: " + collection.size();
        return output;
    }

    @Override
    public void setDeltaReceiver(DeltaReceiver receiver) { 
        deltaReceiver = receiver; 
    }

    @Override
    public void addData(Data data) {

        System.out.print("dataContainer: [");
        for (Data d : dataContainer) {
           System.out.print(d.getDataId() + ", "); 
        }
        System.out.println("] => " + dataContainer.size());

        dataContainer.add(data);
        dataContainer.sort(Comparator.comparingInt(Data::getDataId));

        final List<SimpleEntry<Data, Data>> entries = new ArrayList<>();

        // Find consecutively numbered data IDs
        Data firstData = dataContainer.get(0);
        for (Data secondData : dataContainer ) {

            int desiredID = firstData.getDataId() + 1;
            int actualID = secondData.getDataId();

            System.out.println(
                "{" + Thread.currentThread() + "} " +"dataIDs: " 
                + desiredID + ", " + actualID
            );

            if (desiredID ==  actualID) {

                System.out.println(
                    "{" + Thread.currentThread() + "} " + 
                    "Adding matching DataIDs: " + desiredID + ", " + actualID
                );
                entries.add(new SimpleEntry<>(firstData, secondData));
            }
            firstData = secondData;
        }
        System.out.println(
            "{" + Thread.currentThread() + "} " +"entries: " + 
            formatCollection(entries)
        );

        entries.forEach(entry -> {

            System.out.println(
                "{" + Thread.currentThread() + "} " + "entry(DataIds): (" + 
                entry.getKey().getDataId() + ", " + 
                entry.getValue().getDataId() + ")"
            );

            disaggregateTasks(entry)
                .forEach(task -> { executorService.execute(task); });
        });
    }

    private List<Task> disaggregateTasks(SimpleEntry<Data, Data> dataEntry) {
        List<Task> tasks = new ArrayList<Task>();

        // All tasks have the same length
        int dataSize = dataEntry.getKey().getSize();

        for (int i = 0; i < threads; i++) {
            tasks.add(new Task(dataEntry, i));
        }

        return tasks;
    }

    private class TaskQueue extends PriorityBlockingQueue<Runnable> {
        public TaskQueue(int capacity) {
            super(capacity, new Comparator<Runnable>() {
                @Override
                public int compare(Runnable firstTask, Runnable secondTask) {
                    return Comparator.comparingInt(Task::acquireID).compare(
                        (Task) firstTask, (Task) secondTask
                    );
                }
            });
        }
    }

    private class Task implements Runnable {
        // Fields
        private final int id;
        private final int startFrom;

        private final Data firstData;
        private final Data secondData;

        private final List<Delta> deltas = new ArrayList<Delta>();

        // Methods
        Task(SimpleEntry<Data, Data> dataEntry, int start) {
            startFrom = start;
            firstData = dataEntry.getKey();
            secondData = dataEntry.getValue();
            id = firstData.getDataId();
        }

        // This does all the dirty work AKA computations
        @Override
        public void run() {
            int dataSize = firstData.getSize();
            for (int i = startFrom; i < dataSize; i += threads) {

                int firstValue = firstData.getValue(i);
                int secondValue = secondData.getValue(i);

                if ( firstValue != secondValue ) {
                    System.out.println(
                        "{" + Thread.currentThread() + "} " + 
                        "Adding deltaID: " + id + ", delta: " + 
                        (firstValue - secondValue)
                    );
                    deltas.add(new Delta(id, i, firstValue - secondValue));
                }
            }

            synchronized (dataContainer) {
                deltaReceiver.accept(deltas);

                // for (Data data : dataContainer) {
                //     if (data.getDataId() == ID)
                //         dataContainer.remove(data);
                // }
            }
        }

        public int acquireID() { return id; }
    }

    // Main
    public static void main(String[] args) {
        ParallelCalculator pc = new ParallelCalculator();
        pc.setThreadsNumber(2);
        pc.setDeltaReceiver(new DeltaReceiverExample());

        int data_size = 4;
        for (int i = 0; i < data_size; i++) {

            List<Integer> arr = new ArrayList<Integer>();

            for (int j = 0; j < data_size; j++)
                arr.add(j - i * j);

            /*System.out.println(
                "{" + Thread.currentThread() + "} " +
                "Data[" + i + "]: " + pc.formatCollection(arr)
            );*/

            Data data = new DataExample(arr, i);
            pc.addData(data);
        }
        pc.executorService.shutdown();
    }
}
