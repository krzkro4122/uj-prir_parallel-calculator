import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Vector;
import java.util.List;
import java.util.Set;

public class ParallelCalculator implements DeltaParallelCalculator {

    private interface PrioritizedRunnable extends Runnable {
        public int getPriority();
    }

    private class SendBuffer {
        private List<Delta> deltas = new ArrayList<Delta>();
        private int threadCounter;

        public synchronized void accept(List<Delta> newDeltas){
            this.deltas.addAll(newDeltas);
            if(threadCounter == threadCount - 1){
                Set<Integer> keySet = buffers.keySet();
                for (Integer key : keySet) {
                    if(key == currentPriority){
                        deltaReceiver.accept(buffers.remove(key).deltas);
                        currentPriority++;
                    }
                }
            }
            else{
                threadCounter++;
            }
        }
    }

    private class ComparingWorker implements PrioritizedRunnable {
        private Data dataLeft;
        private Data dataRight;
        private int thread;
        private Vector<Delta> result;
        private int priority;

        public ComparingWorker(Data dataLeft, Data dataRight, int thread){
            this.dataLeft = dataLeft;
            this.dataRight = dataRight;
            this.thread = thread;
            this.result = new Vector<Delta>();
            this.priority = dataLeft.getDataId();
        }

        @Override
        public void run() {
            int valueLeft = 0;
            int valueRight = 0;
            for ( int i = thread; i < dataSize; i+= threadCount) {
                valueLeft = dataLeft.getValue(i);
                valueRight = dataRight.getValue(i);
                if(valueLeft != valueRight){
                    result.add(new Delta(priority, i, valueRight - valueLeft));
                }
            }
            SendBuffer buff = buffers.putIfAbsent(priority, new SendBuffer());
            if(buff == null){
                buffers.get(priority).accept(result);
            }
            else{
                buff.accept(result);
            }
        }

        @Override
        public int getPriority() {
            return this.priority;
        }
    }

    private class DataWrapper implements Data, Cloneable {
        private Data data;
        private boolean isProcessedLeft = false;
        public boolean isProcessedLeft() {
            return isProcessedLeft;
        }

        public void setProcessedLeft(boolean isProcessedLeft) {
            this.isProcessedLeft = isProcessedLeft;
        }

        private boolean isProcessedRight = false;
        public boolean isProcessedRight() {
            return isProcessedRight;
        }

        public void setProcessedRight(boolean isProcessedRight) {
            this.isProcessedRight = isProcessedRight;
        }

        public DataWrapper(Data data){
            this.data = data;
        }

        @Override
        public int getDataId() {
            return data.getDataId();
        }

        @Override
        public int getSize() {
            return data.getSize();
        }

        @Override
        public int getValue(int idx) {
            return data.getValue(idx);
        }
        public Object clone(){
            try{
                return super.clone();
            }catch(Exception e){
                return null;
            }
        }
    }

    private void ScheduleIfPairFound() {
        //Data is already sorted
        DataWrapper dataLeft;
        DataWrapper dataRight;
        for(Integer key: datas.keySet()){
            dataLeft = datas.get(key);
            dataRight = datas.get(key+1);

            if(dataRight != null){
                if(!dataLeft.isProcessedRight() && !dataRight.isProcessedLeft()){
                    dataLeft.setProcessedRight(true);
                    dataRight.setProcessedLeft(true);
                    for(int thread = 0 ; thread < threadCount; thread++){
                        threadPoolExecutor.execute(new ComparingWorker(dataLeft, dataRight, thread));
                    }
                }
            }
            dataLeft = null;
            dataRight = null;
        }
    }

    private ReentrantLock mutex = new ReentrantLock();
    private int currentPriority = 0;
    private int threadCount;
    private ThreadPoolExecutor threadPoolExecutor;
    private DeltaReceiver deltaReceiver;
    private Integer dataSize;
    private ConcurrentMap<Integer, SendBuffer> buffers = new ConcurrentSkipListMap<Integer, SendBuffer>();
    private ConcurrentMap<Integer, DataWrapper> datas = new ConcurrentSkipListMap<Integer, DataWrapper>();

    @Override
    public void setThreadsNumber(int threads) {
        this.threadCount = threads;
        this.threadPoolExecutor = new ThreadPoolExecutor(threads, threads, 100L, TimeUnit.MILLISECONDS,
            new PriorityBlockingQueue<>(10, (o1, o2) -> Comparator.comparingInt(
                PrioritizedRunnable::getPriority).compare((PrioritizedRunnable)o1, (PrioritizedRunnable)o2)
            ));
    }

    @Override
    public void setDeltaReceiver(DeltaReceiver receiver) {
        this.deltaReceiver = receiver;
    }

    @Override
    public synchronized void addData(Data data) {
        if(dataSize == null){
            this.dataSize = data.getSize(); //All data will have the same size
        }
        datas.put(data.getDataId(), (new DataWrapper(data)));
        ScheduleIfPairFound();
    }
}