package org.apache.fluss.fs.abfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryFileSystem extends FileSystem {

    private URI uri;
    private Path workingDir;

    private final Map<Path, byte[]> files = new ConcurrentHashMap<>();
    private final Set<Path> directories = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public MemoryFileSystem(String fsPath) {
        this.uri = URI.create(fsPath);
        this.workingDir = new Path(fsPath);
        directories.add(workingDir);
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.uri = URI.create(name.getScheme() + ":///");
        this.workingDir = new Path("/");
        directories.add(new Path("/"));
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return files.containsKey(f) || directories.contains(f);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    private Path makeAbsolute(Path f) {
        return f.isAbsolute() ? f : new Path(workingDir, f);
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return open(f, -1);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        f = makeAbsolute(f);
        byte[] data = files.get(f);
        if (data == null) throw new FileNotFoundException(f.toString());
        return new FSDataInputStream(
                new FSInputStream() {
                    private int pos = 0;

                    @Override
                    public void seek(long pos) {
                        this.pos = (int) pos;
                    }

                    @Override
                    public long getPos() {
                        return pos;
                    }

                    @Override
                    public boolean seekToNewSource(long targetPos) {
                        return false;
                    }

                    @Override
                    public int read() {
                        return pos < data.length ? (data[pos++] & 0xff) : -1;
                    }
                }
        );
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
        return create(f, overwrite,
                -1, (short) -1,
                -1, null);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite,
                                     int bufferSize, short replication,
                                     long blockSize, Progressable progress) throws IOException {
        f = makeAbsolute(f);

        if (!overwrite && files.containsKey(f)) {
            throw new IOException("File exists: " + f);
        }

        directories.add(f.getParent());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final Path toRet = f;
        return new FSDataOutputStream(new FilterOutputStream(baos) {
            @Override
            public void close() throws IOException {
                super.close();
                files.put(toRet, baos.toByteArray());
            }
        }, null);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable) throws IOException {
        return create(path, b,
                i, i1,
                l, progressable);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        return null;
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        if (files.remove(f) != null) return true;

        if (!recursive) {
            boolean hasChildren = files.keySet().stream().anyMatch(p -> p.getParent().toString().startsWith(f.toString()));
            if (hasChildren) throw new IOException();
        }

        directories.removeIf(d -> d.toString().startsWith(f.toString()));
        files.keySet().removeIf(p -> p.toString().startsWith(f.toString()));
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path f) {
        f = makeAbsolute(f);

        if (files.containsKey(f)) {
            return new FileStatus[]{
                    new FileStatus(files.get(f).length, false, 1, 1, 0, f)
            };
        }

        if (directories.contains(f)) {
            List<FileStatus> statusList = new ArrayList<>();

            // Files
            for (Path p : files.keySet()) {
                if (p.getParent().equals(f)) {
                    statusList.add(new FileStatus(files.get(p).length,
                            false, 1, 1, 0, p));
                }
            }

            // Directories
            for (Path d : directories) {
                if (d.getParent() != null && d.getParent().equals(f) && !d.equals(f)) {
                    statusList.add(new FileStatus(0, true, 1, 1, 0, d));
                }
            }

            return statusList.toArray(new FileStatus[0]);
        }

        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {

    }

    @Override
    public Path getWorkingDirectory() {
        return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        f = makeAbsolute(f);

        Path parent = f;
        while (parent!=null&&!parent.equals(workingDir)) {
            if(files.containsKey(parent)) {
                throw new IOException();
            }
            parent = parent.getParent();
        }


        directories.add(f);
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {

        if (files.containsKey(f)) {
            return new FileStatus(files.get(f).length, false, 1, 1, 0, f);
        }
        if (directories.contains(f)) {
            return new FileStatus(0, true, 1, 1, 0, f);
        }

        throw new FileNotFoundException(f.toString());
    }
}
