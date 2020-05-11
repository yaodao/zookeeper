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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

/*
 *  Used to perform an atomic write into a file.
 *  If there is a failure in the middle of the writing operation, 
 *  the original file (if it exists) is left intact.
 *  Based on the org.apache.zookeeper.server.quorum.QuorumPeer.writeLongToFile(...) idiom
 *  using the HDFS AtomicFileOutputStream class.
 */

/**
 * 本类用于对文件执行原子写操作。
 * 若在写的过程中出现错误，则原文件（若存在）不变
 */
public class AtomicFileWritingIdiom {

    public static interface OutputStreamStatement {

        public void write(OutputStream os) throws IOException;

    }

    public static interface WriterStatement {

        public void write(Writer os) throws IOException;

    }

    // 入参osStmt向targetFile中写入数据
    public AtomicFileWritingIdiom(File targetFile, OutputStreamStatement osStmt)  throws IOException {
        this(targetFile, osStmt, null);
    }

    // 入参wStmt向targetFile中写入数据
    public AtomicFileWritingIdiom(File targetFile, WriterStatement wStmt)  throws IOException {
        this(targetFile, null, wStmt);
    }

    // 入参osStmt对象或者wStmt对象，借助AtomicFileOutputStream流对象， 向targetFile中写入数据，之后关闭流对象
    private AtomicFileWritingIdiom(File targetFile, OutputStreamStatement osStmt, WriterStatement wStmt)  throws IOException {
        AtomicFileOutputStream out = null;
        boolean error = true;
        try {
            // new一个针对targetFile的输出流对象out
            out = new AtomicFileOutputStream(targetFile);
            if (wStmt == null) {
                // execute output stream operation
                // 这里可以间接执行底层流out的write方法
                osStmt.write(out);
            } else {
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
                // execute writer operation and flush
                // 这里可以间接执行底层流bw的write方法
                wStmt.write(bw);
                bw.flush();
            }
            out.flush();
            // everything went ok
            error = false;
        } finally {
            // nothing interesting to do if out == null
            if (out != null) {
                if (error) {
                    // worst case here the tmp file/resources(fd) are not cleaned up
                    // and the caller will be notified (IOException)
                    out.abort();
                } else {
                    // if the close operation (rename) fails we'll get notified.
                    // worst case the tmp file may still exist
                    IOUtils.closeStream(out);
                }
            }
        }
    }

}
