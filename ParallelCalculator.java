import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.Comparator;

class ParallelCalculator implements DeltaParallelCalculator {
    // Fields
    int threads = 0;
    DeltaReceiver deltaReceiver;
    ExecutorService executorService;
    PriorityQueue<Data> priorityQueue;
    CompletionService<ArrayList<Delta>> completionService;

    // Methods
    ParallelCalculator(int threads) {
        this.setThreadsNumber(threads); // TODO -> move this out of constructor maybe
        this.priorityQueue = new PriorityQueue<Data>(new Comparator<Data>() {
                @Override
                public int compare(Data firstData, Data secondData) {
                    return Integer.compare(firstData.getDataId(), secondData.getDataId());
                }
            }
        );
    }

    @Override
    public void setThreadsNumber(int threads) {
        this.executorService = Executors.newFixedThreadPool(this.threads);
        this.completionService = new ExecutorCompletionService<>(executorService);
    }

    @Override
    public void setDeltaReceiver(DeltaReceiver receiver) { this.deltaReceiver = receiver; }

    @Override
    public void addData(Data data) {
        // synchronize with this, is it safe??? no clue!
        synchronized (this) {
            priorityQueue.add(data);
            System.out.println("Added data of ID: " + data.getDataId());
        }
    }

    /*
    *   Continuously poll if there are at least two elements in the queue.
    *   If there are at least 2 elements - create comparison tasks for them.
    *   The Data pairs with the lowest IDs are consumed first.
    */
    public void consumeQueue(PriorityQueue<Data> priorityQueue) {
        System.out.println(priorityQueue);
    }

    // Create a task that compares the consequent data values and returns a list of discrepant data's indeces
    private Callable<ArrayList<Delta>> createTask(final Data data1, final Data data2) {
        return new Callable<ArrayList<Delta>>() {
            // This does all the dirty work AKA computations
            @Override
            public ArrayList<Delta> call() throws Exception {
                ArrayList<Delta> badIndeces = new ArrayList<Delta>();
                // Compare consequent data values.
                // Assumption: both data sets are of the same size
                for (int i = 0; i < data1.getSize(); i++) {
                    int value1 = data1.getValue(i);
                    int value2 = data2.getValue(i);
                    // Add the indeces of missmatched data values to the list
                    if ( value1 != value2 ) {
                        badIndeces.add(new Delta(data1.getDataId(), i, value1 - value2));
                    }
                }
                return badIndeces;
            }
        };
    };

    // Main
    public static void main(String[] args) {
        ParallelCalculator pc = new ParallelCalculator(4);

        pc.completionService.submit(pc.createTask(i));

        boolean errors = false;
        while (!errors) {
            Future<ArrayList<Delta>> response = pc.completionService.take();
            try {
                System.out.println( "Wynik " + response.get() );
            } catch ( Exception e ) {
                errors = true;
                System.out.println("Something went wrong! Error message:" + e.getStackTrace());
            }
        }
        pc.executorService.shutdown();
    }
}
