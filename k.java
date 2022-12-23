import java.util.*;
import java.util.concurrent.*;
// SHAMEROC XD
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ParallelCalculator implements DeltaParallelCalculator {

    ParallelCalculator() {
    }

    private int threads = -1;

    private DeltaReceiver deltaReceiver;

    private ExecutorService executorService;

    List<Data> dataList = new ArrayList<>();

    Set<Pair<Data, Data>> alreadyCalculated = new HashSet<>();

    private final ConcurrentNavigableMap<Integer, Triple<List<Delta>, AtomicInteger, State>> map = new ConcurrentSkipListMap<>();

    private final ReentrantLock mainLock = new ReentrantLock();

    @Override
    public void setThreadsNumber(int threads) {
        this.threads = threads;
        this.executorService = new ThreadPoolExecutor(threads, threads, 100L, MILLISECONDS, new PriorityBlockingQueue<>(11, (o1, o2) -> Comparator.comparingInt(DataComparator::getId)
                                                                                                                                                .compare((DataComparator) o1, (DataComparator) o2)));
    }

    @Override
    public void setDeltaReceiver(DeltaReceiver receiver) {
        this.deltaReceiver = receiver;
    }

    @Override
    public void addData(Data data) {
        dataList.add(data);

        dataList.sort(Comparator.comparingInt(Data::getDataId));

        final List<Pair<Data, Data>> pairList = new ArrayList<>();
        Data previousData = dataList.get(0);
        for (final Data element : dataList) {
            if (previousData.getDataId() + 1 == element.getDataId()) {
                pairList.add(new Pair<>(previousData, element));
            }
            previousData = element;
        }

        pairList.stream()
                .filter(alreadyCalculated::add)
                .map(this::divideToDataComparators)
                .flatMap(List::stream)
                .forEach(executorService::execute);
    }

    private List<DataComparator> divideToDataComparators(Pair<Data, Data> pair) {

        final List<DataComparator> pairList = new ArrayList<>();

        final AtomicInteger finishedProcessCount = new AtomicInteger(0);

        map.put(pair.first.getDataId(), new Triple<>(new ArrayList<>(), new AtomicInteger(0), new State(State.StateEnum.NOT_SENT)));

        final List<Delta> localList = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threads; i++) {
            pairList.add(new DataComparator(deltaReceiver, map, pair, i, threads, finishedProcessCount, localList));
        }

        return pairList;
    }

    private class DataComparator implements Runnable {

        private final DeltaReceiver deltaReceiver;

        private final Data first;

        private final Data second;

        private final int id;

        private final int threads;

        private final ConcurrentNavigableMap<Integer, Triple<List<Delta>, AtomicInteger, State>> globalMap;

        private final List<Delta> localList;

        private final AtomicInteger finishedProcessCount;

        DataComparator(DeltaReceiver deltaReceiver, ConcurrentNavigableMap<Integer, Triple<List<Delta>, AtomicInteger, State>> map, Pair<Data, Data> pair, int id, int threads, AtomicInteger finishedProcessCount, List<Delta> localList) {
            this.deltaReceiver = deltaReceiver;
            this.first = pair.first();
            this.second = pair.second();
            this.globalMap = map;
            this.id = id;
            this.threads = threads;
            this.finishedProcessCount = finishedProcessCount;
            this.localList = localList;
        }


        public int getId() {
            return first.getDataId();
        }

        @Override
        public void run() {
            final var deltaList = populateDeltaList();

            if (!deltaList.isEmpty()) {
                localList.addAll(deltaList);
            }
            if (finishedProcessCount.incrementAndGet() == threads) {
                globalMap.get(getId()).first.addAll(localList);
                globalMap.get(getId()).second.set(threads);

                final List<Map.Entry<Integer, Triple<List<Delta>, AtomicInteger, State>>> toRemove = new ArrayList<>();

                mainLock.lock();
                int previous = -1;
                for (final var s : globalMap.entrySet()) {
                    if (previous + 1 == s.getKey() && s.getValue().second.get() == threads) {
                        if (s.getValue().third.getCurrentState().equals(State.StateEnum.NOT_SENT)) {
                            if (!s.getValue().first.isEmpty()) {
                                synchronized (deltaReceiver) {
                                    deltaReceiver.accept(s.getValue().first());
                                }
                            }

                            toRemove.add(s);
                        }
                        previous = s.getKey();
                    } else {
                        break;
                    }
                }

                toRemove.forEach(e -> e.getValue().third.setState(State.StateEnum.SENT));
                mainLock.unlock();
            }
        }

        private List<Delta> populateDeltaList() {
            final List<Delta> result = new ArrayList<>();
            for (int i = id; i < first.getSize(); i += threads) {
                final int firstValue = first.getValue(i);
                final int secondValue = second.getValue(i);
                if (firstValue != secondValue) {
                    result.add(new Delta(first.getDataId(), i, secondValue - firstValue));
                }
            }
            return result;
        }
    }

    private class State {

        private StateEnum currentState;

        public State(StateEnum state) {
            this.currentState = state;
        }

        public void setState(StateEnum state) {
            this.currentState = state;
        }

        public StateEnum getCurrentState() {
            return currentState;
        }

        public enum StateEnum {
            NOT_SENT, SENT
        }
    }

    private record Pair<T, M>(T first, M second) {}

    private record Triple<T, M, N>(T first, M second, N third) {}
}