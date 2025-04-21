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

import com.alibaba.fluss.fs.token.Credentials;
import com.alibaba.fluss.fs.token.CredentialsJsonSerde;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


/**
 * Tests for the GSTokenReceiver.
 */
class GSTokenReceiverTest {

    @Test
    void testOnNewTokensObtained() throws Exception {
        Credentials credentials = new Credentials(
                null,
                null,
                "token"
        );

        byte[] credentialsBytes = CredentialsJsonSerde.toJson(credentials);

        ObtainedSecurityToken obtainedSecurityToken = new ObtainedSecurityToken(
                "gs",
                credentialsBytes,
                10L,
                new HashMap<>());

        GSTokenReceiver gsTokenReceiver = new GSTokenReceiver();
        gsTokenReceiver.onNewTokensObtained(obtainedSecurityToken);

        Credentials receivedCredentials = GSTokenReceiver.getCredentials();
        assertThat(receivedCredentials.getSecurityToken()).isEqualTo("token");
    }

}