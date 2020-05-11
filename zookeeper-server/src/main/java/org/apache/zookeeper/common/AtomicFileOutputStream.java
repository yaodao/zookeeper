/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This code is originally from HDFS, see the similarly named files there
 * in case of bug fixing, history, etc...
 */

/**
 * A FileOutputStream that has the property that it will only show up at its
 * destination once it has been entirely written and flushed to disk. While
 * being written, it will use a .tmp suffix.
 *
 * When the output stream is closed, it is flushed, fsynced, and will be moved
 * into place, overwriting any file that already exists at that location.
 *
 * <b>NOTE</b>: on Windows platforms, it will not atomically replace the target
 * file - instead the target file is deleted before this one is moved into
 * place.
 */

/**
 * FileOutputStream流对象，
 * 它的特点是，只有在内容被完全写入并刷新到磁盘之后，文件才会出现在文件夹中。
 * 在正在写入时，文件将使用.tmp后缀。
 * 当输出流关闭时，它将被移动到适当的位置，并覆盖该位置上已有的同名文件。
 * 注意:在Windows平台上，它不会自动替换目标文件——除非先删除已有的同名文件。
 */
public class AtomicFileOutputStream extends FilterOutputStream {
    private static final String TMP_EXTENSION = ".tmp";

    private final static Logger LOG = LoggerFactory
            .getLogger(AtomicFileOutputStream.class);

    private final File origFile;
    private final File tmpFile;

    // 给底层输出流、origFile、tmpFile赋值
    public AtomicFileOutputStream(File f) throws FileNotFoundException {
        // Code unfortunately must be duplicated below since we can't assign
        // anything
        // before calling super
        super(new FileOutputStream(new File(f.getParentFile(), f.getName()
                + TMP_EXTENSION)));
        origFile = f.getAbsoluteFile();
        tmpFile = new File(f.getParentFile(), f.getName() + TMP_EXTENSION)
                .getAbsoluteFile();
    }

    /**
     * The default write method in FilterOutputStream does not call the write
     * method of its underlying input stream with the same arguments. Instead
     * it writes the data byte by byte, override it here to make it more
     * efficient.
     */
    /**
     * FilterOutputStream默认的write方法并不是使用相同的参数调用它的底层输入流的写方法。
     * 因为底层输入流是一个字节一个字节地写数据，所以在这里重写这个write方法，以使它更有效。
     */
    @Override
    public void write(byte b[], int off, int len) throws IOException {
        out.write(b, off, len);
    }

    // 关闭out流，并给.tmp文件转正（重命名.tmp文件）
    @Override
    public void close() throws IOException {
        // triedToClose 是否将缓存flush到输出流；  success 是否成功关闭流
        boolean triedToClose = false, success = false;
        try {
            // 将缓存中的字节写入输出流
            flush();
            // sync()方法阻塞直到数据缓冲区的数据全部写入磁盘,该方法返回后，数据已经写入到磁盘
            ((FileOutputStream) out).getFD().sync();

            triedToClose = true;
            // 关闭被包装的那个流
            // （我理解关闭底层这个被包装的流，就相当于关闭所有的流了。因为上层并没有创建新流，一直用的是底层这个流）
            super.close();
            success = true;
        } finally {
            // 若成功关闭out流，则修改.tmp的文件名
            if (success) {
                boolean renamed = tmpFile.renameTo(origFile);
                if (!renamed) {
                    // On windows, renameTo does not replace.
                    // 这里的判断用的挺妙，一石多鸟。
                    // 若删除文件失败，或者重命名文件失败，都会抛出异常
                    // 若删除文件成功，则继续执行一次重命名操作。挺有意思。
                    if (!origFile.delete() || !tmpFile.renameTo(origFile)) {
                        throw new IOException(
                                "Could not rename temporary file " + tmpFile
                                        + " to " + origFile);
                    }
                }
            }
            // 若关闭out流 失败，则删除.tmp文件
            else {
                // 若try中flush流失败
                if (!triedToClose) {
                    // If we failed when flushing, try to close it to not leak
                    // an FD
                    // 关闭out流
                    IOUtils.closeStream(out);
                }
                // close wasn't successful, try to delete the tmp file
                // 删除.tmp文件
                if (!tmpFile.delete()) {
                    LOG.warn("Unable to delete tmp file " + tmpFile);
                }
            }
        }
    }

    /**
     * Close the atomic file, but do not "commit" the temporary file on top of
     * the destination. This should be used if there is a failure in writing.
     */
    // 关闭流，并删除.tmp文件
    public void abort() {
        try {
            super.close();
        } catch (IOException ioe) {
            LOG.warn("Unable to abort file " + tmpFile, ioe);
        }
        if (!tmpFile.delete()) {
            LOG.warn("Unable to delete tmp file during abort " + tmpFile);
        }
    }
}
