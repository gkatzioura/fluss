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

package com.alibaba.fluss.fs.gs.token;

import com.alibaba.fluss.fs.gs.GSFileSystemPlugin;
import com.alibaba.fluss.fs.gs.MockAuthServer;
import com.alibaba.fluss.fs.token.Credentials;
import com.alibaba.fluss.fs.token.CredentialsJsonSerde;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;


/** Tests for the GSTokenProvider. */
class GSTokenProviderTest {

    private MockAuthServer mockGSServer;

    @BeforeEach
    void setUp() {
        mockGSServer = MockAuthServer.create();
    }

    @Test
    void testObtainSecurityToken() throws Exception {
        String path =
                GSFileSystemPlugin.class
                        .getClassLoader()
                        .getResource("fake-service-account.json")
                        .getPath();

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        conf.set("fs.gs.token.server.url", "http://localhost:8080/token");
        conf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");
        conf.set("fs.gs.auth.service.account.json.keyfile", path);
        conf.set("fs.gs.inputstream.support.gzip.encoding.enable", "false");

        GSTokenProvider gsTokenProvider = new GSTokenProvider("gs",conf);
        ObtainedSecurityToken obtainedSecurityToken = gsTokenProvider.obtainSecurityToken();
        assertThat(obtainedSecurityToken).isNotNull();
        assertThat(obtainedSecurityToken.getScheme()).isEqualTo("gs");

        Credentials credentials = CredentialsJsonSerde.fromJson(obtainedSecurityToken.getToken());
        assertThat(credentials.getSecurityToken()).isEqualTo("token");
        assertThat(obtainedSecurityToken.getValidUntil().isPresent()).isTrue();
    }

    @AfterEach
    void tearDown() {
        mockGSServer.close();
    }
}