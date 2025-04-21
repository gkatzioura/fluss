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

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.fs.token.CredentialsJsonSerde;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.fs.gs.token.GSTokenReceiver.PROVIDER_CONFIG_NAME;

/** A provider to provide access tokens for Google Cloud Storage. */
public class GSTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(GSTokenProvider.class);

    private final String scheme;
    private final Configuration configuration;

    public GSTokenProvider(String scheme, Configuration configuration) {
        this.scheme = scheme;
        this.configuration = configuration;
    }

    public static void updateHadoopConfig(org.apache.hadoop.conf.Configuration hadoopConfig) {
        LOG.info("Updating Hadoop configuration");

        String providers = hadoopConfig.get(PROVIDER_CONFIG_NAME, "");
        if (!providers.contains(DynamicTemporaryAWSCredentialsProvider.NAME)) {
            if (providers.isEmpty()) {
                LOG.debug("Setting provider");
                providers = DynamicTemporaryAWSCredentialsProvider.NAME;
            } else {
                providers = DynamicTemporaryAWSCredentialsProvider.NAME + "," + providers;
                LOG.debug("Prepending provider, new providers value: {}", providers);
            }
            hadoopConfig.set(PROVIDER_CONFIG_NAME, providers);
        } else {
            LOG.debug("Provider already exists");
        }

        // then, set addition info
        if (additionInfos == null) {
            // if addition info is null, it also means we have not received any token,
            // we throw InvalidCredentialsException
            throw new NoAwsCredentialsException(DynamicTemporaryAWSCredentialsProvider.COMPONENT);
        } else {
            for (Map.Entry<String, String> entry : additionInfos.entrySet()) {
                hadoopConfig.set(entry.getKey(), entry.getValue());
            }
        }

        LOG.info("Updated Hadoop configuration successfully");
    }

    /**
     * Obtains the access token, there is no refresh token involved thus a refresh due to an expiration equals to creating a new token.
     * @return
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        Credential credential = createCredential();

        return new ObtainedSecurityToken(
                scheme,
                toJson(credential),
                credential.getExpirationTimeMilliseconds(),
                new HashMap<>()
        );
    }

    private Credential createCredential() throws IOException {
        try {
            Credential credential = HadoopCredentialConfiguration.getCredentialFactory(configuration, GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX).getCredential(CredentialFactory.DEFAULT_SCOPES);
            credential.refreshToken();
            return credential;
        } catch (GeneralSecurityException e) {
            throw new FlussRuntimeException(e);
        }
    }

    private byte[] toJson(Credential credential) {
        com.alibaba.fluss.fs.token.Credentials flussCredentials =
                new com.alibaba.fluss.fs.token.Credentials(
                        null,
                        null,
                        credential.getAccessToken());
        return CredentialsJsonSerde.toJson(flussCredentials);
    }

}