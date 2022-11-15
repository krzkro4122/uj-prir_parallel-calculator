import java.util.concurrent.*;

class ParallelCalculator implements Callable<Integer>, DeltaParallelCalculator {
    // Fields
    int threads = 0;
    DeltaReceiver deltaReceiver;

    // Methods
    ParallelCalculator(int threads) { this.setThreadsNumber(threads); }

    @Override
    public void setThreadsNumber(int threads) { this.threads = threads; }

    @Override
    public void setDeltaReceiver(DeltaReceiver receiver) { this.deltaReceiver = receiver; }

    @Override
    public void addData(Data data) {}

    @Override
    public Integer call() throws Exception {
        Thread.currentThread().yield();
        System.out.println("[" + Thread.currentThread().getName() + "] I am saying shit!");
        Thread.currentThread().yield();
        return 2;
    }

    // Main
    public static void main(String[] args) {
        System.out.println("It's-a me Javio!");

        ParallelCalculator pc = new ParallelCalculator(4);
        ExecutorService executorService = Executors.newFixedThreadPool(pc.threads);
        CompletionService<Integer> completionService = new ExecutorCompletionService<>(executorService);

        completionService.submit(pc);

        Future<Integer> response;
        try {
            response = completionService.take();
            System.out.println( "Wynik " + response.get() );
        } catch ( Exception e ) {
            System.out.println("Error response?");
        }
        executorService.shutdown();
    }
}
