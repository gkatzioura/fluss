/*
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

package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FileSystemPlugin;
import com.alibaba.fluss.fs.gs.utils.ConfigUtils;
import com.alibaba.fluss.utils.Preconditions;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;

/** Simple factory for the Google Cloud Storage file system. */
public class GSFileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(GSFileSystemPlugin.class);

    private static final String[] FLUSS_CONFIG_PREFIXES = {"gs.", "fs.gs."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.gs.";

    /** The scheme for the Google Storage file system. */
    public static final String SCHEME = "gs";

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri, Configuration configuration) throws IOException {
        Preconditions.checkNotNull(configuration);

        ConfigUtils.ConfigContext configContext = new RuntimeConfigContext();

        org.apache.hadoop.conf.Configuration hadoopConfig = ConfigUtils.getHadoopConfiguration(configuration, configContext);

        LOG.info(
                "Using Hadoop configuration {}", ConfigUtils.stringifyHadoopConfig(hadoopConfig));

        GSFileSystemOptions fileSystemOptions = new GSFileSystemOptions(configuration);
        LOG.info("Using file system options {}", fileSystemOptions);

        StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
        storageOptionsBuilder.setTransportOptions(getHttpTransportOptions(fileSystemOptions));
        storageOptionsBuilder.setRetrySettings(getRetrySettings(fileSystemOptions));

        Optional<GoogleCredentials> credentials =
                ConfigUtils.getStorageCredentials(hadoopConfig, configContext);
        credentials.ifPresent(storageOptionsBuilder::setCredentials);

        ConfigUtils.getGcsRootUrl(hadoopConfig).ifPresent(storageOptionsBuilder::setHost);

        Storage storage = storageOptionsBuilder.build().getService();

        Preconditions.checkNotNull(fsUri);

        Preconditions.checkNotNull(fsUri);

        GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();
        try {
            googleHadoopFileSystem.initialize(fsUri, hadoopConfig);
        } catch (IOException ex) {
            throw new IOException("Failed to initialize GoogleHadoopFileSystem", ex);
        }

        return new GSFileSystem(googleHadoopFileSystem, storage, fileSystemOptions);
    }

    private HttpTransportOptions getHttpTransportOptions(GSFileSystemOptions fileSystemOptions) {
        Optional<Integer> connectionTimeout = fileSystemOptions.getHTTPConnectionTimeout();
        Optional<Integer> readTimeout = fileSystemOptions.getHTTPReadTimeout();
        HttpTransportOptions.Builder httpTransportOptionsBuilder =
                HttpTransportOptions.newBuilder();
        connectionTimeout.ifPresent(httpTransportOptionsBuilder::setConnectTimeout);
        readTimeout.ifPresent(httpTransportOptionsBuilder::setReadTimeout);
        return httpTransportOptionsBuilder.build();
    }

    private RetrySettings getRetrySettings(GSFileSystemOptions fileSystemOptions) {
        Optional<Integer> maxAttempts = fileSystemOptions.getMaxAttempts();
        Optional<org.threeten.bp.Duration> initialRpcTimeout =
                fileSystemOptions.getInitialRpcTimeout();
        Optional<Double> rpcTimeoutMultiplier = fileSystemOptions.getRpcTimeoutMultiplier();
        Optional<org.threeten.bp.Duration> maxRpcTimeout = fileSystemOptions.getMaxRpcTimeout();
        Optional<org.threeten.bp.Duration> totalTimeout = fileSystemOptions.getTotalTimeout();
        RetrySettings.Builder retrySettingsBuilder =
                ServiceOptions.getDefaultRetrySettings().toBuilder();

        maxAttempts.ifPresent(retrySettingsBuilder::setMaxAttempts);
        initialRpcTimeout.ifPresent(retrySettingsBuilder::setInitialRpcTimeout);
        rpcTimeoutMultiplier.ifPresent(retrySettingsBuilder::setRpcTimeoutMultiplier);
        maxRpcTimeout.ifPresent(retrySettingsBuilder::setMaxRpcTimeout);
        totalTimeout.ifPresent(retrySettingsBuilder::setTotalTimeout);
        return retrySettingsBuilder.build();
    }

    /** Config context implementation used at runtime. */
    private static class RuntimeConfigContext implements ConfigUtils.ConfigContext {

        @Override
        public Optional<String> getenv(String name) {
            return Optional.ofNullable(System.getenv(name));
        }

        @Override
        public org.apache.hadoop.conf.Configuration loadHadoopConfigFromDir(String configDir) {
            org.apache.hadoop.conf.Configuration hadoopConfig =
                    new org.apache.hadoop.conf.Configuration();
            hadoopConfig.addResource(new Path(configDir, "core-default.xml"));
            hadoopConfig.addResource(new Path(configDir, "core-site.xml"));
            hadoopConfig.reloadConfiguration();
            return hadoopConfig;
        }

        @Override
        public GoogleCredentials loadStorageCredentialsFromFile(String credentialsPath) {
            try (FileInputStream credentialsStream = new FileInputStream(credentialsPath)) {
                return GoogleCredentials.fromStream(credentialsStream);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
