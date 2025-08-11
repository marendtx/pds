import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        Config config = new Config();
        KVStore kv = new KVStore(config);
        kv.put("title", "Microservices");
        kv.put("author", "Martin");
        WriteBatch batch = new WriteBatch();
        batch.put("year", "2015");
        batch.put("publisher", "SomePub");
        kv.put(batch);
        KVStore kv2 = new KVStore(config);
        System.out.println("title= " + kv2.get("title"));
        System.out.println("author= " + kv2.get("author"));
        System.out.println("year= " + kv2.get("year"));
        System.out.println("publisher= " + kv2.get("publisher"));
    }
}

class Config {}

enum EntryType { COMMAND }

final class WALEntry {
    private final Long entryIndex;
    private final byte[] data;
    private final EntryType entryType;
    private final long timeStamp;
    public WALEntry(Long entryIndex, byte[] data, EntryType entryType, long timeStamp) {
        this.entryIndex = entryIndex;
        this.data = data;
        this.entryType = entryType;
        this.timeStamp = timeStamp;
    }
    public Long getEntryIndex() { return entryIndex; }
    public byte[] getData() { return data; }
    public EntryType getEntryType() { return entryType; }
    public long getTimeStamp() { return timeStamp; }
}

class WriteAheadLog {
    private final List<WALEntry> entries = new ArrayList<>();
    private long nextIndex = 1L;
    public static WriteAheadLog openWAL(Config ignored) {
        return WalSingleton.INSTANCE;
    }
    public synchronized Long writeEntry(byte[] payload) {
        long now = Instant.now().toEpochMilli();
        WALEntry e = new WALEntry(nextIndex++, payload, EntryType.COMMAND, now);
        entries.add(e);
        return e.getEntryIndex();
    }
    public synchronized List<WALEntry> readAll() {
        return new ArrayList<>(entries);
    }
    private static class WalSingleton {
        static final WriteAheadLog INSTANCE = new WriteAheadLog();
    }
}

interface Command {
    int type();
    void serialize(DataOutputStream os) throws IOException;
    static byte[] toBytes(Command c) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(c.type());
            c.serialize(dos);
            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

final class CommandTypes {
    static final int SetValueType = 1;
    static final int WriteBatchType = 2;
}

class SetValueCommand implements Command {
    final String key;
    final String value;
    public SetValueCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }
    @Override public int type() { return CommandTypes.SetValueType; }
    @Override
    public void serialize(DataOutputStream os) throws IOException {
        os.writeUTF(key);
        os.writeUTF(value);
    }
    public static SetValueCommand deserialize(DataInputStream is) {
        try {
            return new SetValueCommand(is.readUTF(), is.readUTF());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

class WriteBatchCommand implements Command {
    private final WriteBatch batch;
    public WriteBatchCommand(WriteBatch batch) {
        this.batch = batch;
    }
    public WriteBatch getBatch() { return batch; }
    @Override public int type() { return CommandTypes.WriteBatchType; }
    @Override
    public void serialize(DataOutputStream os) throws IOException {
        Map<String,String> kv = batch.kv;
        os.writeInt(kv.size());
        for (Map.Entry<String,String> e : kv.entrySet()) {
            os.writeUTF(e.getKey());
            os.writeUTF(e.getValue());
        }
    }
    public static WriteBatchCommand deserialize(DataInputStream is) {
        try {
            int n = is.readInt();
            WriteBatch b = new WriteBatch();
            for (int i = 0; i < n; i++) {
                String k = is.readUTF();
                String v = is.readUTF();
                b.put(k, v);
            }
            return new WriteBatchCommand(b);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

final class CommandCodec {
    public static Command deserialize(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bis)) {
            int type = dis.readInt();
            return switch (type) {
                case CommandTypes.SetValueType -> SetValueCommand.deserialize(dis);
                case CommandTypes.WriteBatchType -> WriteBatchCommand.deserialize(dis);
                default -> throw new IllegalArgumentException("Unknown command type: " + type);
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

class WriteBatch {
    final Map<String, String> kv = new LinkedHashMap<>();
    public void put(String key, String value) {
        kv.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
    }
}

class KVStore {
    private final Config config;
    private final WriteAheadLog wal;
    private final Map<String, String> kv = new HashMap<>();
    public KVStore(Config config) {
        this.config = config;
        this.wal = WriteAheadLog.openWAL(config);
        this.applyLog();
    }
    public String get(String key) { return kv.get(key); }
    public void put(String key, String value) {
        appendLog(key, value);
        kv.put(key, value);
    }
    public void put(WriteBatch batch) {
        appendLog(batch);
        kv.putAll(batch.kv);
    }
    private Long appendLog(String key, String value) {
        SetValueCommand cmd = new SetValueCommand(key, value);
        return wal.writeEntry(Command.toBytes(cmd));
    }
    private Long appendLog(WriteBatch batch) {
        WriteBatchCommand cmd = new WriteBatchCommand(batch);
        return wal.writeEntry(Command.toBytes(cmd));
    }
    private void applyLog() {
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
        applyBatchLogEntries(walEntries);
    }
    private void applyEntries(List<WALEntry> walEntries) {
        for (WALEntry walEntry : walEntries) {
            Command command = CommandCodec.deserialize(walEntry.getData());
            if (command instanceof SetValueCommand svc) {
                kv.put(svc.key, svc.value);
            }
        }
    }
    private void applyBatchLogEntries(List<WALEntry> walEntries) {
        for (WALEntry walEntry : walEntries) {
            Command command = CommandCodec.deserialize(walEntry.getData());
            if (command instanceof WriteBatchCommand batchCommand) {
                WriteBatch batch = batchCommand.getBatch();
                kv.putAll(batch.kv);
            }
        }
    }
}
