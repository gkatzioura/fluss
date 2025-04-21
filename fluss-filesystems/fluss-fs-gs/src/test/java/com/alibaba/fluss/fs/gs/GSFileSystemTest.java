/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests that covers the method of the GSFileSystem. */
class GSFileSystemTest {

    private MockAuthServer mockGSServer;

    @BeforeEach
    void setUp() {
        mockGSServer = MockAuthServer.create();
    }

    @Test
    void testObtainSecurityToken() throws IOException {
        String path =
                GSFileSystemPlugin.class
                        .getClassLoader()
                        .getResource("fake-service-account.json")
                        .getPath();

        Configuration configuration = new Configuration();
        configuration.set("fs.gs.storage.root.url", "http://localhost:8080");
        configuration.set("fs.gs.token.server.url", "http://localhost:8080/token");
        configuration.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
        configuration.set("fs.gs.auth.service.account.json.keyfile", path);
        configuration.set("fs.gs.inputstream.support.gzip.encoding.enable", "false");

        GSFileSystem gsFileSystem = new GSFileSystem("gs",new GoogleHadoopFileSystem(), configuration);
        ObtainedSecurityToken token = gsFileSystem.obtainSecurityToken();
        assertThat(token).isNotNull();
    }

    @AfterEach
    void tearDown() {
        mockGSServer.close();
    }
}