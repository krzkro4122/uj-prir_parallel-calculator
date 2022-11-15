import java.util.concurrent.*;

class ParallelCalculator implements Callable<Void>, DeltaParallelCalculator {
    // Fields
    Thread[] threads;
    DeltaReceiver deltaReceiver;

    // Methods
    ParallelCalculator(int threads) {
        setThreadsNumber(threads);
    }

    public void setThreadsNumber(int threads) {
        for (int i = 0; i < threads; i++) {
            this.threads[i] = new Thread();
        }
    }

    public void setDeltaReceiver(DeltaReceiver receiver) {
        this.deltaReceiver = receiver;
    }

    public void addData(Data data) {}

    public List< call() throws Exception {
        System.out.println("[" + Thread.currentThread().getName() + "] I am saying shit!");
        return new Void();
    }

    // Main
    public static void main(String[] args) {
        System.out.println("It's-a me Javio!");
        ParallelCalculator pc = new ParallelCalculator(4);

        ExecutorService executorService = Executors.newCachedThreadPool();
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        completionService.submit(pc);
    }
}
