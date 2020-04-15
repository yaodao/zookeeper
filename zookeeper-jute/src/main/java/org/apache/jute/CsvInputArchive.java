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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.UnsupportedEncodingException;

/**
 *
 */
class CsvInputArchive implements InputArchive {
    // 可读取字符的流（允许字符被重新推回流中）
    private PushbackReader stream;
    
    private class CsvIndex implements Index {
        // 从stream读取一个字符，判断该字符是否为"}"，是则返回true
        public boolean done() {
            char c = '\0';
            try {
                c = (char) stream.read();
                stream.unread(c);
            } catch (IOException ex) {
            }
            return (c == '}') ? true : false;
        }
        public void incr() {}
    }

    // 从stream读取一个字段 （入参tag就是log用）
    private String readField(String tag) throws IOException {
        try {
            StringBuilder buf = new StringBuilder();
            while (true) {
                char c = (char) stream.read();
                switch (c) {
                    case ',':
                        // 遇到逗号，则返回已读取的值
                        return buf.toString();
                    case '}':
                    case '\n':
                    case '\r':
                        // 遇到以上几个符号，则返回已读取的值

                        // 推回缓冲区，也就说下次read还是读到这个字符。
                        stream.unread(c);
                        return buf.toString();
                    default:
                        buf.append(c);
                }
            }
        } catch (IOException ex) {
            throw new IOException("Error reading "+tag);
        }
    }
    
    static CsvInputArchive getArchive(InputStream strm)
    throws UnsupportedEncodingException {
        return new CsvInputArchive(strm);
    }
    
    /** Creates a new instance of CsvInputArchive */
    // 创建一个CsvInputArchive对象（就是new一个输入流 赋值给成员变量stream）
    public CsvInputArchive(InputStream in)
    throws UnsupportedEncodingException {
        stream = new PushbackReader(new InputStreamReader(in, "UTF-8"));
    }
    
    public byte readByte(String tag) throws IOException {
        return (byte) readLong(tag);
    }
    
    public boolean readBool(String tag) throws IOException {
        String sval = readField(tag);
        return "T".equals(sval) ? true : false;
    }
    
    public int readInt(String tag) throws IOException {
        return (int) readLong(tag);
    }

    // 从stream读取一个long值（入参tag用于log）
    public long readLong(String tag) throws IOException {
        String sval = readField(tag);
        try {
            long lval = Long.parseLong(sval);
            return lval;
        } catch (NumberFormatException ex) {
            throw new IOException("Error deserializing "+tag);
        }
    }

    // 从stream读取一个float值（入参tag用于log）
    public float readFloat(String tag) throws IOException {
        return (float) readDouble(tag);
    }

    // 从stream读取一个double值（入参tag用于log）
    public double readDouble(String tag) throws IOException {
        String sval = readField(tag);
        try {
            double dval = Double.parseDouble(sval);
            return dval;
        } catch (NumberFormatException ex) {
            throw new IOException("Error deserializing "+tag);
        }
    }

    // 从stream读取一个字符串，并将其中的%**字符替换成对应的字符
    public String readString(String tag) throws IOException {
        String sval = readField(tag);
        return Utils.fromCSVString(sval);
        
    }

    // 从stream读取一个字符串，并将其中每两个字符转成一个byte，返回转化后的byte数组
    public byte[] readBuffer(String tag) throws IOException {
        String sval = readField(tag);
        return Utils.fromCSVBuffer(sval);
    }
    
    public void readRecord(Record r, String tag) throws IOException {
        r.deserialize(this, tag);
    }

    // 当入参tag不为空时， 若stream的前两个字符不是's'或'{' 则抛出异常。
    public void startRecord(String tag) throws IOException {
        if (tag != null && !"".equals(tag)) {
            char c1 = (char) stream.read();
            char c2 = (char) stream.read();
            if (c1 != 's' || c2 != '{') {
                throw new IOException("Error deserializing "+tag);
            }
        }
    }

    // 当入参tag为空时， 若stream的第一个字符不是'\n'或'\r' 则抛出异常。
    // 当入参tag不为空，若stream的第一个字符不是'}' 则抛出异常
    public void endRecord(String tag) throws IOException {
        char c = (char) stream.read();
        if (tag == null || "".equals(tag)) {
            if (c != '\n' && c != '\r') {
                throw new IOException("Error deserializing record.");
            } else {
                return;
            }
        }
        
        if (c != '}') {
            throw new IOException("Error deserializing "+tag);
        }
        c = (char) stream.read();
        if (c != ',') {
            stream.unread(c);
        }
        
        return;
    }
    
    public Index startVector(String tag) throws IOException {
        char c1 = (char) stream.read();
        char c2 = (char) stream.read();
        if (c1 != 'v' || c2 != '{') {
            throw new IOException("Error deserializing "+tag);
        }
        return new CsvIndex();
    }
    
    public void endVector(String tag) throws IOException {
        char c = (char) stream.read();
        if (c != '}') {
            throw new IOException("Error deserializing "+tag);
        }
        c = (char) stream.read();
        if (c != ',') {
            stream.unread(c);
        }
        return;
    }
    
    public Index startMap(String tag) throws IOException {
        char c1 = (char) stream.read();
        char c2 = (char) stream.read();
        if (c1 != 'm' || c2 != '{') {
            throw new IOException("Error deserializing "+tag);
        }
        return new CsvIndex();
    }
    
    public void endMap(String tag) throws IOException {
        char c = (char) stream.read();
        if (c != '}') {
            throw new IOException("Error deserializing "+tag);
        }
        c = (char) stream.read();
        if (c != ',') {
            stream.unread(c);
        }
        return;
    }
}
