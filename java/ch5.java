import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws Exception {
        File dir = new File("./wal");
        dir.mkdirs();
        Config cfg = new Config(dir, 3, 200, 1000, CleanerType.SNAPSHOT);
        WriteAheadLog wal = new WriteAheadLog(cfg);
        KVStore store = new KVStore(wal);
        for (int i = 1; i <= 10; i++) { store.put("k"+i, "v"+i); Thread.sleep(50); }
        SnapShot snap = store.takeSnapshot();
        Thread.sleep(1200);
        Config cfg2 = new Config(dir, 3, 200, 500, CleanerType.TIME);
        WriteAheadLog walTime = new WriteAheadLog(cfg2);
        KVStore store2 = new KVStore(walTime);
        for (int i = 11; i <= 16; i++) { store2.put("k"+i, "v"+i); Thread.sleep(50); }
        Thread.sleep(1500);
    }
}

class WALEntry {
    private final Long entryIndex;
    private final byte[] data;
    private final long timestamp;
    public WALEntry(Long entryIndex, byte[] data, long timestamp) {
        this.entryIndex = entryIndex;
        this.data = data;
        this.timestamp = timestamp;
    }
    public Long getEntryIndex() { return entryIndex; }
    public byte[] getData() { return data; }
    public long getTimestamp() { return timestamp; }
}

class WALSegment {
    private static final String logPrefix = "wal";
    private static final String logSuffix = ".log";
    private final Long baseOffset;
    private final List<WALEntry> entries = new ArrayList<>();
    public WALSegment(Long baseOffset) { this.baseOffset = baseOffset; }
    public static WALSegment open(Long baseOffset, File dir) { return new WALSegment(baseOffset); }
    public static String createFileName(Long startIndex) { return logPrefix + "_" + startIndex + logSuffix; }
    public static Long getBaseOffsetFromFileName(String fileName) {
        String[] nameAndSuffix = fileName.split(logSuffix);
        String[] prefixAndOffset = nameAndSuffix[0].split("_");
        if (prefixAndOffset[0].equals(logPrefix)) return Long.parseLong(prefixAndOffset[1]);
        return -1L;
    }
    public Long writeEntry(WALEntry entry) { entries.add(entry); return entry.getEntryIndex(); }
    public long size() { return entries.size(); }
    public void flush() {}
    public Long getBaseOffset() { return baseOffset; }
    public Long getLastLogEntryIndex() { return entries.isEmpty() ? baseOffset : entries.get(entries.size()-1).getEntryIndex(); }
    public Long getLastLogEntryTimestamp() { return entries.isEmpty() ? 0L : entries.get(entries.size()-1).getTimestamp(); }
    public List<WALEntry> getEntriesFrom(Long startExclusive) {
        List<WALEntry> out = new ArrayList<>();
        for (WALEntry e : entries) if (e.getEntryIndex() > startExclusive) out.add(e);
        return out;
    }
}

class Config {
    private final File walDir;
    private final long maxLogSize;
    private final long cleanTaskIntervalMs;
    private final long logMaxDurationMs;
    private final CleanerType cleanerType;
    public Config(File walDir, long maxLogSize, long cleanTaskIntervalMs, long logMaxDurationMs, CleanerType cleanerType) {
        this.walDir = walDir; this.maxLogSize = maxLogSize; this.cleanTaskIntervalMs = cleanTaskIntervalMs; this.logMaxDurationMs = logMaxDurationMs; this.cleanerType = cleanerType;
    }
    public File getWalDir() { return walDir; }
    public long getMaxLogSize() { return maxLogSize; }
    public long getCleanTaskIntervalMs() { return cleanTaskIntervalMs; }
    public long getLogMaxDurationMs() { return logMaxDurationMs; }
    public CleanerType getCleanerType() { return cleanerType; }
}

enum CleanerType { SNAPSHOT, TIME }

abstract class LogCleaner {
    protected final WriteAheadLog wal;
    protected final Config config;
    private final ScheduledExecutorService singleThreadedExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "wal-cleaner"); t.setDaemon(true); return t;
    });
    public LogCleaner(WriteAheadLog wal, Config config) { this.wal = wal; this.config = config; }
    public void startup() { scheduleLogCleaning(); }
    private void scheduleLogCleaning() {
        singleThreadedExecutor.schedule(() -> { cleanLogs(); }, config.getCleanTaskIntervalMs(), TimeUnit.MILLISECONDS);
    }
    public void cleanLogs() {
        List<WALSegment> segmentsTobeDeleted = getSegmentsToBeDeleted();
        for (WALSegment walSegment : segmentsTobeDeleted) { wal.removeAndDeleteSegment(walSegment); }
        scheduleLogCleaning();
    }
    abstract List<WALSegment> getSegmentsToBeDeleted();
}

class SnapshotBasedLogCleaner extends LogCleaner {
    private volatile Long snapshotIndex;
    public SnapshotBasedLogCleaner(WriteAheadLog wal, Config config) { super(wal, config); this.snapshotIndex = 0L; }
    public void setSnapshotIndex(Long snapshotIndex) { this.snapshotIndex = snapshotIndex == null ? 0L : snapshotIndex; }
    @Override List<WALSegment> getSegmentsToBeDeleted() { return getSegmentsBefore(this.snapshotIndex); }
    List<WALSegment> getSegmentsBefore(Long snapshotIndex) {
        List<WALSegment> markedForDeletion = new ArrayList<>();
        List<WALSegment> sortedSavedSegments = wal.sortedSavedSegments;
        for (WALSegment sortedSavedSegment : sortedSavedSegments) {
            if (sortedSavedSegment.getLastLogEntryIndex() < snapshotIndex) { markedForDeletion.add(sortedSavedSegment); }
        }
        return markedForDeletion;
    }
}

class TimeBasedLogCleaner extends LogCleaner {
    public TimeBasedLogCleaner(WriteAheadLog wal, Config config) { super(wal, config); }
    @Override List<WALSegment> getSegmentsToBeDeleted() { return getSegmentsPast(config.getLogMaxDurationMs()); }
    private List<WALSegment> getSegmentsPast(Long logMaxDurationMs) {
        long now = System.currentTimeMillis();
        List<WALSegment> markedForDeletion = new ArrayList<>();
        List<WALSegment> sortedSavedSegments = wal.sortedSavedSegments;
        for (WALSegment sortedSavedSegment : sortedSavedSegments) {
            Long lastTimestamp = sortedSavedSegment.getLastLogEntryTimestamp();
            if (timeElaspedSince(now, lastTimestamp) > logMaxDurationMs) { markedForDeletion.add(sortedSavedSegment); }
        }
        return markedForDeletion;
    }
    private long timeElaspedSince(long now, long lastLogEntryTimestamp) { return now - lastLogEntryTimestamp; }
}

class WriteAheadLog {
    final List<WALSegment> sortedSavedSegments = new ArrayList<>();
    private WALSegment openSegment;
    private final Config config;
    private final LogCleaner logCleaner;
    public WriteAheadLog(Config config) {
        this.config = config;
        this.openSegment = WALSegment.open(0L, config.getWalDir());
        this.logCleaner = newLogCleaner(config);
        this.logCleaner.startup();
    }
    private LogCleaner newLogCleaner(Config config) {
        if (config.getCleanerType() == CleanerType.SNAPSHOT) return new SnapshotBasedLogCleaner(this, config);
        return new TimeBasedLogCleaner(this, config);
    }
    public Long writeEntry(WALEntry entry) {
        maybeRoll();
        return openSegment.writeEntry(entry);
    }
    private void maybeRoll() {
        if (openSegment.size() >= config.getMaxLogSize()) {
            openSegment.flush();
            sortedSavedSegments.add(openSegment);
            long lastId = openSegment.getLastLogEntryIndex();
            openSegment = WALSegment.open(lastId, config.getWalDir());
        }
    }
    public List<WALEntry> readFrom(Long startIndex) {
        List<WALSegment> segments = getAllSegmentsContainingLogGreaterThan(startIndex);
        return readWalEntriesFrom(startIndex, segments);
    }
    private List<WALSegment> getAllSegmentsContainingLogGreaterThan(Long startIndex) {
        List<WALSegment> segments = new ArrayList<>();
        for (int i = sortedSavedSegments.size() - 1; i >= 0; i--) {
            WALSegment walSegment = sortedSavedSegments.get(i);
            segments.add(walSegment);
            if (walSegment.getBaseOffset() <= startIndex) { break; }
        }
        if (openSegment.getBaseOffset() <= startIndex) { segments.add(openSegment); }
        return segments;
    }
    private List<WALEntry> readWalEntriesFrom(Long startIndex, List<WALSegment> segments) {
        List<WALEntry> out = new ArrayList<>();
        for (WALSegment s : segments) out.addAll(s.getEntriesFrom(startIndex));
        return out;
    }
    public Long getLastLogIndex() {
        Long max = openSegment.getLastLogEntryIndex();
        for (WALSegment s : sortedSavedSegments) max = Math.max(max, s.getLastLogEntryIndex());
        return max;
    }
    public void removeAndDeleteSegment(WALSegment seg) { sortedSavedSegments.remove(seg); }
    public void updateSnapshotIndex(Long idx) { if (logCleaner instanceof SnapshotBasedLogCleaner sc) sc.setSnapshotIndex(idx); }
}

class SnapShot {
    private final byte[] data;
    private final Long snapshotIndex;
    public SnapShot(byte[] data, Long snapshotIndex) { this.data = data; this.snapshotIndex = snapshotIndex; }
    public byte[] getData() { return data; }
    public Long getSnapshotIndex() { return snapshotIndex; }
}

class KVStore {
    private final Map<String,String> kv = new HashMap<>();
    private final WriteAheadLog wal;
    public KVStore(WriteAheadLog wal) { this.wal = wal; }
    public void put(String k, String v) {
        long ts = System.currentTimeMillis();
        Long index = wal.getLastLogIndex() + 1;
        wal.writeEntry(new WALEntry(index, encode(k, v), ts));
        kv.put(k, v);
    }
    public String get(String k) { return kv.get(k); }
    public SnapShot takeSnapshot() {
        Long snapShotTakenAtLogIndex = wal.getLastLogIndex();
        SnapShot s = new SnapShot(serializeState(kv), snapShotTakenAtLogIndex);
        wal.updateSnapshotIndex(s.getSnapshotIndex());
        return s;
    }
    private byte[] serializeState(Map<String,String> map) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeInt(map.size());
            for (Map.Entry<String,String> e : map.entrySet()) { dos.writeUTF(e.getKey()); dos.writeUTF(e.getValue()); }
            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }
    private byte[] encode(String k, String v) { return (k+"="+v).getBytes(); }
}


