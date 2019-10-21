/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.consistency.jraft.snapshot;

import com.alibaba.nacos.consistency.jraft.BizDomain;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author satjd
 */
public class OperationSnapshotFile {

    private static final Logger LOG = LoggerFactory.getLogger(OperationSnapshotFile.class);

    private String              path;

    private ObjectMapper mapper = new ObjectMapper();

    public OperationSnapshotFile(String path) {
        super();
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    /**
     * Save value to snapshot file.
     */
    public boolean save(final long value) {
        try {
            FileUtils.writeStringToFile(new File(path), String.valueOf(value));
            return true;
        } catch (IOException e) {
            LOG.error("Fail to save snapshot", e);
            return false;
        }
    }

    public boolean save (Map<String, BizDomain> bizDomainMap) {
        try {
            FileUtils.writeStringToFile(new File(path), mapper.writeValueAsString(bizDomainMap));
            return true;
        } catch (IOException e) {
            LOG.error("Fail to save snapshot", e);
            return false;
        }
    }

    public Map<String,BizDomain> load() throws IOException {
        final String s = FileUtils.readFileToString(new File(path));
        if (!StringUtils.isBlank(s)) {
            TypeReference<Map<String,BizDomain>> ref =new TypeReference<Map<String, BizDomain>>() {};
            return mapper.readValue(s, ref);
        }
        throw new IOException("Fail to load snapshot from " + path + ",content: " + s);
    }
}
