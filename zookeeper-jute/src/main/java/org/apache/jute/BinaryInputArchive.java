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

package org.apache.jute;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class BinaryInputArchive implements InputArchive {
    // log用
    public static final String UNREASONBLE_LENGTH= "Unreasonable length = ";
    public static final int maxBuffer = Integer.getInteger("jute.maxbuffer", 0xfffff);
    private static final int extraMaxBuffer;

    // 给成员变量extraMaxBuffer赋值
    static {
        final Integer configuredExtraMaxBuffer =
            Integer.getInteger("zookeeper.jute.maxbuffer.extrasize", maxBuffer);
        if (configuredExtraMaxBuffer < 1024) {
            // Earlier hard coded value was 1024, So the value should not be less than that value
            extraMaxBuffer = 1024;
        } else {
            extraMaxBuffer = configuredExtraMaxBuffer;
        }
    }
    // 输入流
    // 用于从该输入流读取字节并构造成Java基本类型的值，包括String类型的值
    private DataInput in;
    private int maxBufferSize;
    private int extraMaxBufferSize;
    
    static public BinaryInputArchive getArchive(InputStream strm) {
        return new BinaryInputArchive(new DataInputStream(strm));
    }
    
    static private class BinaryIndex implements Index {
        private int nelems;
        BinaryIndex(int nelems) {
            this.nelems = nelems;
        }
        public boolean done() {
            return (nelems <= 0);
        }
        public void incr() {
            nelems--;
        }
    }
    /** Creates a new instance of BinaryInputArchive */
    public BinaryInputArchive(DataInput in) {
        this(in, maxBuffer, extraMaxBuffer);
    }

    public BinaryInputArchive(DataInput in, int maxBufferSize, int extraMaxBufferSize) {
        this.in = in;
        this.maxBufferSize = maxBufferSize;
        this.extraMaxBufferSize = extraMaxBufferSize;
    }

    // 从in流读取一个字节
    public byte readByte(String tag) throws IOException {
        return in.readByte();
    }
    
    public boolean readBool(String tag) throws IOException {
        return in.readBoolean();
    }

    // 从in流读取一个int值
    public int readInt(String tag) throws IOException {
        return in.readInt();
    }
    
    public long readLong(String tag) throws IOException {
        return in.readLong();
    }
    
    public float readFloat(String tag) throws IOException {
        return in.readFloat();
    }
    
    public double readDouble(String tag) throws IOException {
        return in.readDouble();
    }

    // 从in流读取String类型的值
    public String readString(String tag) throws IOException {
        // String的长度
    	int len = in.readInt();
    	if (len == -1) return null;
        checkLength(len);
    	byte b[] = new byte[len];
        // 从输入流中读取len个字节，并将它们存储在数组b中
    	in.readFully(b);
    	return new String(b, "UTF8");
    }

    // 从in流读取byte数组
    public byte[] readBuffer(String tag) throws IOException {
        int len = readInt(tag);
        if (len == -1) return null;
        checkLength(len);
        byte[] arr = new byte[len];
        // 从输入流中读取len个字节，并将它们存储在数组b中
        in.readFully(arr);
        return arr;
    }
    
    public void readRecord(Record r, String tag) throws IOException {
        r.deserialize(this, tag);
    }
    
    public void startRecord(String tag) throws IOException {}
    
    public void endRecord(String tag) throws IOException {}

    // 读取向量
    public Index startVector(String tag) throws IOException {
        // 向量长度
        int len = readInt(tag);
        if (len == -1) {
        	return null;
        }
		return new BinaryIndex(len);
    }
    
    public void endVector(String tag) throws IOException {}

    // 读取map
    public Index startMap(String tag) throws IOException {
        return new BinaryIndex(readInt(tag));
    }
    
    public void endMap(String tag) throws IOException {}

    // Since this is a rough sanity check, add some padding to maxBuffer to
    // make up for extra fields, etc. (otherwise e.g. clients may be able to
    // write buffers larger than we can read from disk!)
    private void checkLength(int len) throws IOException {
        if (len < 0 || len > maxBufferSize + extraMaxBufferSize) {
            throw new IOException(UNREASONBLE_LENGTH + len);
        }
    }
}
