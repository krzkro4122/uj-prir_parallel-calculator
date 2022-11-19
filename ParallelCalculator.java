import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;

import java.util.NoSuchElementException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;

class Task implements Runnable {
    // Fields
    Data firstData, secondData;
    // Methods
    Task(Data firstData, Data secondData) {
        this.firstData = firstData;
        this.secondData =  secondData;
    }
    // This does all the dirty work AKA computations
    @Override
    public void run() {
        System.out.println("[call] Starting...");
        ArrayList<Delta> badIndeces = new ArrayList<Delta>();
        // Compare consequent data values.
        // Assumption: both data sets are of the same size
        for (int i = 0; i < firstData.getSize(); i++) {
            System.out.println("[call] i: " + i);
            int value1 = firstData.getValue(i);
            int value2 = secondData.getValue(i);
            // Add the resulting Delta created from the missmatched data values and index to the list
            if ( value1 != value2 ) { badIndeces.add(new Delta(firstData.getDataId(), i, value1 - value2)); }
        }
    }
};

class ParallelCalculator implements DeltaParallelCalculator {
    // Fields
    int threads = 0;
    DeltaReceiver deltaReceiver;
    ExecutorService executorService;
    PriorityBlockingQueue<Runnable> taskQueue;
    // CompletionService<ArrayList<Delta>> completionService;

    // Methods
    ParallelCalculator() {
        // Define the Data queue container
        taskQueue = new PriorityBlockingQueue<>(this.threads * 2, new Comparator<Data>() {
            @Override
            public int compare(Data firstData, Data secondData) {
                return Integer.compare(firstData.getDataId(), secondData.getDataId());
            }
        });
        // Initialize the continuous receiver function
        consumeQueue();
    }

    @Override
    public void setThreadsNumber(int threads) {
        this.threads = threads;
        executorService = new ThreadPoolExecutor(this.threads, this.threads, 0L, TimeUnit.SECONDS, taskQueue);
        completionService = new ExecutorCompletionService<>(executorService);
    }

    @Override
    public void setDeltaReceiver(DeltaReceiver receiver) { deltaReceiver = receiver; }

    @Override
    public void addData(Data data) {
        taskQueue.add(data);
        System.out.println("[Queue] Added data: ID -> " + data.getDataId());
    }

    /*
    *   Continuously poll if there are at least two elements in the queue.
    *   If there are at least 2 elements - create comparison tasks for them.
    *   The Data pairs with the lowest IDs are consumed first.
    */
    public void consumeQueue() {
        // Set to be done
        // Custom Kolejka ktora
        try {
            System.out.println("[" + Thread.currentThread() + "] Consuming! PriorityQueue[len=" + taskQueue.size() + "]: " + taskQueue);
            if (taskQueue.size() > 1) {
                Data firstData = taskQueue.remove();
                Data secondData = taskQueue.remove();

                // No dissagregatrion just yet! >:D
                // TODO - queue parsing and Data set dissagregation stuff woof woof

                System.out.println("[consumeQueue] Using dataID: " + firstData.getDataId() +
                    " and " + secondData.getDataId() +  " to create a task...");
                Task task = createTask(firstData, secondData);

                System.out.println("[consumeQueue] Created a task: " + task + " - Submitting and getting...");
                // Future<ArrayList<Delta>> response = completionService.submit(task);
                deltaReceiver.accept(completionService.submit(task).get()); // ???

                // System.out.println("[consumeQueue] Submitted a task:  Taking...");
                // response = completionService.take();

                // System.out.println("[consumeQueue] Task response: " + response.get());
                // synchronized (this) { deltaReceiver.accept(response.get()); }
            }
        } catch (NoSuchElementException nsee) {
            System.out.println("NoSuchElementException caught! - " + nsee.getStackTrace());
        } catch (InterruptedException ie) {
            System.out.println("InterruptedException caught! - " + ie.getStackTrace());
            return;
        } catch (ExecutionException ee) {
            System.out.println("ExecutionException caught! - " + ee.getStackTrace());
        } catch (RejectedExecutionException ree) {
            System.out.println("Rejected??? " + ree.getStackTrace()); // TODO ???
        }
    }

    // Create a task that compares the consequent data values and returns a list of discrepant data's indices
    private Task createTask(final SimpleEntry<Data, Data> dataEntry) {
        System.out.println("[createTask] Starting...");
        return new Task(dataEntry.getKey(), dataEntry.getValue());
    };

    // Main
    public static void main(String[] args) {
        ParallelCalculator pc = new ParallelCalculator();
        pc.setThreadsNumber(4);
        pc.setDeltaReceiver(
            new DeltaReceiver() {
                @Override
                public void accept(List<Delta> deltas) { System.out.println("ziomal..."); }
            }
        );

        for (int i = 0; i < 20; i++) {
            int[] arr = new int[8];
            for (int j = 0; j < 8; j++) { arr[j] = j + i; }
            Data data = new DataExample(arr, i);
            pc.addData(data);
        }
        pc.executorService.shutdown();
    }
}
