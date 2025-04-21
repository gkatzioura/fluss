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
import com.alibaba.fluss.fs.token.SecurityTokenReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GSTokenReceiver implements SecurityTokenReceiver {

    public static final String PROVIDER_CONFIG_NAME = "fs.gs.gcp.credentials.provider";
    private static final Logger LOG = LoggerFactory.getLogger(GSTokenReceiver.class);

    static volatile Credentials credentials;
    static volatile Map<String, String> additionInfos;

    @Override
    public String scheme() {
        return "gs";
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) throws Exception {
        LOG.info("Updating session credentials");

        byte[] tokenBytes = token.getToken();

        synchronized (this) {
            credentials = CredentialsJsonSerde.fromJson(tokenBytes);
            additionInfos = token.getAdditionInfos();
        }

        LOG.info(
                "Session credentials updated successfully with access key: {}.",
                credentials.getAccessKeyId());
    }

    public static Credentials getCredentials() {
        return credentials;
    }

}
