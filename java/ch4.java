import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        Config config = new Config();
        WriteAheadLog wal = new WriteAheadLog(config);
        for (int i = 1; i <= 12; i++) {
            wal.writeEntry(new WALEntry((long) i, ("data" + i).getBytes()));
        }
        List<WALEntry> entries = wal.readFrom(5L);
        for (WALEntry e : entries) {
            System.out.println(e.getEntryIndex() + " -> " + new String(e.getData()));
        }
    }
}

class WALEntry {
    private final Long entryIndex;
    private final byte[] data;
    public WALEntry(Long entryIndex, byte[] data) {
        this.entryIndex = entryIndex;
        this.data = data;
    }
    public Long getEntryIndex() { return entryIndex; }
    public byte[] getData() { return data; }
}

class WALSegment {
    private static final String logPrefix = "wal";
    private static final String logSuffix = ".log";
    private final Long baseOffset;
    private final List<WALEntry> entries = new ArrayList<>();
    private final File file;
    public WALSegment(Long baseOffset, File dir) {
        this.baseOffset = baseOffset;
        this.file = new File(dir, createFileName(baseOffset));
    }
    public static WALSegment open(Long baseOffset, File dir) {
        return new WALSegment(baseOffset, dir);
    }
    public static String createFileName(Long startIndex) {
        return logPrefix + "_" + startIndex + logSuffix;
    }
    public static Long getBaseOffsetFromFileName(String fileName) {
        String[] nameAndSuffix = fileName.split(logSuffix);
        String[] prefixAndOffset = nameAndSuffix[0].split("_");
        if (prefixAndOffset[0].equals(logPrefix))
            return Long.parseLong(prefixAndOffset[1]);
        return -1L;
    }
    public Long writeEntry(WALEntry entry) {
        entries.add(entry);
        return entry.getEntryIndex();
    }
    public long size() {
        return entries.size();
    }
    public void flush() {}
    public Long getLastLogEntryIndex() {
        if (entries.isEmpty()) return baseOffset;
        return entries.get(entries.size() - 1).getEntryIndex();
    }
    public Long getBaseOffset() { return baseOffset; }
    public List<WALEntry> getEntriesFrom(Long startIndex) {
        List<WALEntry> result = new ArrayList<>();
        for (WALEntry e : entries) {
            if (e.getEntryIndex() > startIndex) {
                result.add(e);
            }
        }
        return result;
    }
}

class WriteAheadLog {
    private final Config config;
    private WALSegment openSegment;
    private final List<WALSegment> sortedSavedSegments = new ArrayList<>();
    public WriteAheadLog(Config config) {
        this.config = config;
        this.openSegment = WALSegment.open(0L, config.getWalDir());
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
            if (walSegment.getBaseOffset() <= startIndex) {
                break;
            }
        }
        if (openSegment.getBaseOffset() <= startIndex) {
            segments.add(openSegment);
        }
        return segments;
    }
    private List<WALEntry> readWalEntriesFrom(Long startIndex, List<WALSegment> segments) {
        List<WALEntry> result = new ArrayList<>();
        for (WALSegment seg : segments) {
            result.addAll(seg.getEntriesFrom(startIndex));
        }
        return result;
    }
}

class Config {
    private final File walDir = new File("./wal");
    private final long maxLogSize = 5;
    public File getWalDir() { return walDir; }
    public long getMaxLogSize() { return maxLogSize; }
}
