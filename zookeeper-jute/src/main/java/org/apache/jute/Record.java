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

import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

/**
 * Interface that is implemented by generated classes.
 *
 * 所有的序列化对象都要实现Record接口，它定义了serialize和deserialize方法。
 * 该接口的子类需要实现这两个方法，用于自定义（定制）自己的序列化方式和反序列化方式。
 *
 * 我认为实现了这个接口就是表明它的子类对象可以被序列化和反序列化，
 */
@InterfaceAudience.Public
public interface Record {
    /**
     * 序列化方法，在该方法中，决定子类中哪些字段需要序列化。
     *
     * @param archive 将序列化后的byte写入该输出流
     * @param tag 一个标识符，暂时没用
     * @throws IOException
     */
    public void serialize(OutputArchive archive, String tag)
        throws IOException;

    /**
     * 反序列化方法，在该方法中，决定子类中哪些字段需要在反序列化时读出。
     *
     * @param archive 从该输入流读取byte数据，并转成各种类型的数据
     * @param tag 一个标识符，暂时没用
     * @throws IOException
     */
    public void deserialize(InputArchive archive, String tag)
        throws IOException;
}
