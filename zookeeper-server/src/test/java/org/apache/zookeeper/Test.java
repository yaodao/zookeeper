package org.apache.zookeeper;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.server.ByteBufferInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

// 以下例子，测试了Record子类对象的序列化和反序列化
public class Test {
    public static void main(String[] args) throws IOException {
        RequestHeader requestHeader = new RequestHeader(1, ZooDefs.OpCode.create);
        System.out.print("requestHeader:  " +requestHeader );

        // 输出流
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryOutputArchive binaryOutputArchive = BinaryOutputArchive.getArchive(outputStream);
        // 将requestHeader对象序列化到binaryOutputArchive流中
        requestHeader.serialize(binaryOutputArchive,"header");

        byte[] arr = outputStream.toByteArray();
        System.out.println(arr.length);


        // 这里模拟从网络或者文件中得到一些字节，
        // 先将这些字节包装成ByteBuffer对象，再反序列化成对象
        ByteBuffer bb = ByteBuffer.wrap(outputStream.toByteArray());

        // 输入流
        ByteBufferInputStream inputStream = new ByteBufferInputStream(bb);
        BinaryInputArchive binaryInputArchive =  BinaryInputArchive.getArchive(inputStream);
        // 从binaryInputArchive流中读取数据，反序列化为requestHeader1对象
        RequestHeader requestHeader1 = new RequestHeader();
        requestHeader1.deserialize(binaryInputArchive,"header");
        System.out.print("requestHeader1:  " + requestHeader1);


        outputStream.close();
        inputStream.close();
    }
}
