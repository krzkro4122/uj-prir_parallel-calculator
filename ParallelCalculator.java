import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import java.util.AbstractMap.SimpleEntry;

import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

class ParallelCalculator implements DeltaParallelCalculator {

    // Fields
    private int threads = 0;

    private DeltaReceiver deltaReceiver;
    private ExecutorService executorService;

    private List<Data> dataContainer = new ArrayList<>();
    private List<Delta> deltaContainer = new ArrayList<>();
    private Map<Integer, Integer> threadReports = new HashMap<>();
    private AtomicInteger lowestIdToSend = new AtomicInteger(0);
    private HashSet<SimpleEntry<Data, Data>> alreadyConsumed = new HashSet<>();

    // Methods
    @Override
    public void setThreadsNumber(int threads) {

        this.threads = threads;

        executorService = new ThreadPoolExecutor(
            this.threads,
            this.threads,
            0L,
            TimeUnit.SECONDS,
            new TaskQueue<>(this.threads * 5, (firstTask, secondTask) -> {
                return Comparator.comparingInt(Task::acquireID).compare( (Task) firstTask, (Task) secondTask );
            })
        );
    }

    @Override
    public void setDeltaReceiver(DeltaReceiver receiver) {deltaReceiver = receiver; }

    @Override
    public void addData(Data data) {

        dataContainer.add(data);
        dataContainer.sort(Comparator.comparingInt(Data::getDataId));

        // { // print TODO
        //     String temp = "";
        //     temp += "DATA dataContainer: [";
        //     for (Data d : dataContainer) { temp += d.getDataId() + ", "; }
        //     System.out.println(temp + "] => " + dataContainer.size());
        // }

        // Guard statement
        if (dataContainer.size() <= 1)
            return;

        final List<SimpleEntry<Data, Data>> entries = new ArrayList<>();

        // Find consecutively numbered data IDs
        Data firstData = dataContainer.get(0);

        for (Data d : dataContainer) {

            Data secondData = d;

            int actualID = secondData.getDataId();
            int desiredID = firstData.getDataId() + 1;

            // System.out.println("CHECKING if dataIDs: " + (desiredID - 1) + "+1 == " + actualID); // TODO

            if (desiredID == actualID) {
                // System.out.println("FOUND dataIDs: " + (desiredID - 1) + ", " + actualID); // TODO

                SimpleEntry<Data, Data> foundEntry = new SimpleEntry<>(firstData, secondData);

                if ( !alreadyConsumed.contains(foundEntry)) {
                    entries.add(foundEntry);
                }
            }
            firstData = secondData;
        }

        // { // print TODO
        //     String temp = "";
        //     temp += "entries<DataID, DataID>: [";
        //     for (SimpleEntry<Data, Data> se : entries) {
        //         temp += "(" + se.getKey().getDataId() + ",";
        //         temp += se.getValue().getDataId() + "), ";
        //     }
        //     System.out.println(temp + "] => " + entries.size());
        // }

        entries.forEach(entry -> {
            disaggregateTasks(entry).forEach(task -> {
                executorService.execute(task);
            });
            // executorService.execute(new Task(entry, entry.getKey().getDataId()));
        });
    }

    private List<Task> disaggregateTasks(SimpleEntry<Data, Data> dataEntry) {
        List<Task> tasks = new ArrayList<Task>();

        // TODO ???
        // All tasks have the same length
        // int dataSize = dataEntry.getKey().getSize();

        int idx = dataEntry.getKey().getDataId();
        threadReports.merge(idx, 0, Integer::sum);

        // System.out.println("************** threadReports: " + threadReports);

        for (int i = 0; i < threads; i++) {
            // System.out.println("---- ADDING dataEntry: " + dataEntry.getKey().getDataId() + ", startingPoint: " + i);
            tasks.add(new Task(dataEntry, i));
        }

        return tasks;
    }

    private String data2string(Data data, String name) {
        String output = "\t[" + Thread.currentThread().getId() + "] GOTTEN " + name + ": {dataID: " + data.getDataId() + ", data: [";
        for (int i = 0; i < data.getSize(); i++) {
            if (i == data.getSize() - 1)
                output += data.getValue(i);
            else
                output += data.getValue(i) + ",  ";
        }
        output += "], dataSize: " + data.getSize() + "}";
        return output;
    }

    private class TaskQueue<T> extends PriorityBlockingQueue<T> {

        private final AtomicInteger lowestIdToGrab = new AtomicInteger(0);
        private final ReentrantLock reentrantLock = new ReentrantLock();

        public TaskQueue(int capacity, Comparator<? super T> comparator) {
            super(capacity, comparator);
        }

        private T findT() {

            for (T t : this)
                if ( ( (Task) t ).acquireID() == lowestIdToGrab.get())
                    return t;

            return null;
        }

        // Copied and modified the ORIGINAL take method from PriorityBlockingQueue
        // @Override
        // public T take() throws InterruptedException {

        //     if ( ! this.isEmpty() ) {
        //         String output = "[QUEUE] TAKING tasks from queue:";
        //         for (final var element : this) {
        //             output +=
        //                 "\n  {taskId: " + ( (Task) element ).acquireID() + ", " +
        //                 "startFrom: " + ( (Task) element).startFrom + ", " +
        //                 "dataId: " + ( (Task) element).firstData.getDataId() + "}";
        //         }
        //         System.out.println(output);
        //     }

        //     // Engage lock
        //     final ReentrantLock lock = this.reentrantLock;
        //     lock.lockInterruptibly();

        //     try {
        //         final T topOfQueue = peek();

        //         if ( topOfQueue == null ) {
        //             return poll();
        //         } else if ( ( (Task) topOfQueue ).acquireID() == lowestIdToGrab.get() ) {
        //             return poll();
        //         }

        //         // Find the currently lowest, unprocessed Task' ID
        //         final T foundT = findT();
        //         System.out.println("anybody here? --------- ");

        //         if (foundT == null) {
        //             final T newT = poll();
        //             if (newT != null) {
        //                 lowestIdToGrab.set( ( (Task) newT).acquireID() );
        //                 return newT;
        //             }
        //         } else {
        //             remove(foundT);
        //             return foundT;
        //         }
        //     } finally {
        //         lock.unlock();
        //     }
        //     return null;
        // }
    }

    private class Task implements Runnable {
        // Fields
        private final int id;
        private final int startFrom;

        private Data firstData;
        private Data secondData;

        private final SimpleEntry<Data, Data> currentEntry;
        private final List<Delta> deltas = new ArrayList<Delta>();

        // Methods
        Task(SimpleEntry<Data, Data> dataEntry, int start) {

            startFrom = start;
            currentEntry = dataEntry;
            firstData = dataEntry.getKey();
            secondData = dataEntry.getValue();

            if (firstData.getDataId() > secondData.getDataId()) {
                Data temp = firstData;
                firstData = secondData;
                secondData = temp;
            }

            id = firstData.getDataId();
        }

        // This does all the dirty work AKA computations
        @Override
        public void run() {

            // print both datas first
            // System.out.println(data2string(firstData, "firstData"));
            // System.out.println(data2string(secondData, "secondData"));

            int dataSize = firstData.getSize();

            for (int i = startFrom; i < dataSize; i += threads) {

                int firstValue = firstData.getValue(i);
                int secondValue = secondData.getValue(i);

                if ( firstValue == secondValue )
                    continue;

                // System.out.println(
                //     "\t[" + Thread.currentThread().getId() + "]" +
                //     " - DELTA found: {deltaID: " + id +
                //     ", deltaIndex: " + i +
                //     ", deltaValue: " + (firstValue - secondValue) + " (" + firstValue + " - " + secondValue + ")}"
                // );

                deltas.add(new Delta(id, i, firstValue - secondValue));
            }

            synchronized (deltaContainer) {

                System.out.println("[" + Thread.currentThread().getId() + "]" + threadReports);

                if (threadReports.get(id) != null && threadReports.get(id) != threads)
                    threadReports.merge(id, 1, Integer::sum);
                for (Delta delta : deltas) {
                    if ( !deltaContainer.contains(delta)) {
                        deltaContainer.add(delta);
                    }
                }

                tryToSendDeltas(currentEntry);
            }
        }

        public int acquireID() { return id; }
    }

    private void tryToSendDeltas(SimpleEntry<Data, Data> entry) {
        if (deltaContainer.isEmpty())
            return;

        if (threadReports.get(lowestIdToSend.get()) == null)
            return;

        if (threadReports.get(lowestIdToSend.get()) != threads)
            return;

        List<Delta> oneIdDeltas = new ArrayList<>();

        for (Delta delta : deltaContainer)
            if (delta.getDataID() == lowestIdToSend.get())
                oneIdDeltas.add(delta);

        if ( !alreadyConsumed.contains(entry) ) {
            alreadyConsumed.add(entry);
            deltaReceiver.accept(oneIdDeltas);
            lowestIdToSend.incrementAndGet();
        }
    }

    // Main
    public static void main(String[] args) {
        ParallelCalculator pc = new ParallelCalculator();
        pc.setThreadsNumber(5);
        pc.setDeltaReceiver(new DeltaReceiverExample());

        List<Integer> indeces = List.of(5, 4, 3, 2, 1, 0);

        // Mock data
        int data_size = indeces.size();
        for (int i = 0; i < data_size; i++) {

            List<Integer> list = new ArrayList<Integer>();

            for (int j = 0; j < data_size; j++)
                list.add(j + i);

            // System.out.println(
            //     "ADDING id: " + indeces.get(i) +  " data: " + list
            // );

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
        pc.executorService.shutdown();
    }
}
