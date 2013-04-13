package org.mapdb.extra;

import java.io.EOFException;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.mapdb.DataInput2;
import org.mapdb.Volume;

public class AsyncFileChannelVolume extends Volume{


        protected AsynchronousFileChannel channel;
        protected final boolean readOnly;
        protected final File file;

        public AsyncFileChannelVolume(File file, boolean readOnly){
            this.readOnly = readOnly;
            this.file = file;
            try {
                this.channel = readOnly?
                        AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ):
                        AsynchronousFileChannel.open(file.toPath(),StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void ensureAvailable(long offset) {
            //we do not have a list of ByteBuffers, so ensure size does not have to do anything
        }

        protected void writeFully(long offset, ByteBuffer b, int size) {
            try {
                int written = 0;
                while(written<size){
                    written+=channel.write(b, offset+written).get();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        protected void readFully(long offset, ByteBuffer b, int size) {
            try {
                int read = 0;
                while(read<size){
                    int i = channel.read(b, offset+read).get();
                    if(i==-1) throw new IOError(new EOFException());
                    read+=i;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        protected void await(Future<Integer> future, int size) {
        }

        @Override
        public void putByte(long offset, byte value) {
            ByteBuffer b = ByteBuffer.allocate(1);
            b.put(0, value);
            writeFully(offset, b, 1);
        }
        @Override
        public void putInt(long offset, int value) {
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(0, value);
            writeFully(offset, b, 4);
        }

        @Override
        public void putLong(long offset, long value) {
            ByteBuffer b = ByteBuffer.allocate(8);
            b.putLong(0, value);
            writeFully(offset, b, 8);
        }

        @Override
        public void putData(final long offset, final byte[] src, int srcPos, int srcSize){
            ByteBuffer b = ByteBuffer.wrap(src,srcPos, srcSize);
            writeFully(offset, b, srcSize);
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
            writeFully(offset, buf, buf.limit() - buf.position());
        }



        @Override
        public long getLong(long offset) {
            ByteBuffer b = ByteBuffer.allocate(8);
            readFully(offset, b, 8);
            b.rewind();
            return b.getLong();
        }

        @Override
        public byte getByte(long offset) {
            ByteBuffer b = ByteBuffer.allocate(1);
            readFully(offset, b, 1);
            b.rewind();
            return b.get();
        }

        @Override
        public int getInt(long offset) {
            ByteBuffer b = ByteBuffer.allocate(4);
            readFully(offset, b, 4);
            b.rewind();
            return b.getInt();
        }



        @Override
        public DataInput2 getDataInput(long offset, int size) {
            ByteBuffer b = ByteBuffer.allocate(size);
            readFully(offset, b, size);
            b.rewind();
            return new DataInput2(b,0);
        }

        @Override
        public void close() {
            try {
                channel.close();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void sync() {
            try {
                channel.force(true);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public boolean isEmpty() {
            return file.length()<8;
        }

        @Override
        public void deleteFile() {
            file.delete();
        }

        @Override
        public boolean isSliced() {
            return false;
        }

        @Override
        public File getFile() {
            return file;
        }
    }
