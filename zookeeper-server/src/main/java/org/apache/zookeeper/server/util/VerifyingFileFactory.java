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

package org.apache.zookeeper.server.util;

import java.io.File;

import org.slf4j.Logger;

// 验证文件是否有效的类，感觉这个验证文件的用法可以借鉴引申
public final class VerifyingFileFactory {

    // 需要验证的项
    private final boolean warnForRelativePath;
    private final boolean failForNonExistingPath;

    private final Logger log;

    public VerifyingFileFactory(Builder builder){
        warnForRelativePath = builder.warnForRelativePathOption;
        failForNonExistingPath  = builder.failForNonExistingPathOption;
        log = builder.log;
        assert(log != null);
    }

    // 创建file对象，并验证该file对象是否有效，若有效，则返回该file对象。否则抛出异常
    public File create(String path) {
        File file = new File(path);
        return validate(file);
    }

    // 验证file对象是否满足条件
    public File validate(File file) {
        // 验证
        if(warnForRelativePath) doWarnForRelativePath(file);
        if(failForNonExistingPath) doFailForNonExistingPath(file);
        return file;
    }

    private void doFailForNonExistingPath(File file) {
        if (!file.exists()) {
            throw new IllegalArgumentException(file.toString()
                    + " file is missing");
        }
    }

    // 若file对象的路径是绝对路径 或者 路径以./开头，则返回，否则warn
    private void doWarnForRelativePath(File file) {
        if(file.isAbsolute()) return;
        if(file.getPath().substring(0, 2).equals("."+File.separator)) return;
        log.warn(file.getPath()+" is relative. Prepend ."
                +File.separator+" to indicate that you're sure!");
    }

    public static class Builder {
        private boolean warnForRelativePathOption = false;
        private boolean failForNonExistingPathOption = false;
        private final Logger log;

        public Builder(Logger log){
            this.log = log;
        }

        public Builder warnForRelativePath() {
            warnForRelativePathOption = true;
            return this;
        }

        public Builder failForNonExistingPath() {
            failForNonExistingPathOption = true;
            return this;
        }

        public VerifyingFileFactory build() {
            return new VerifyingFileFactory(this);
        }
    }
}
