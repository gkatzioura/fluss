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

import com.alibaba.fluss.annotation.VisibleForTesting;
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
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystemFactory} interface for
 * Google Storage.
 */
public class GSFileSystemFactory implements FileSystemPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSFileSystemFactory.class);

    /** The scheme for the Google Storage file system. */
    public static final String SCHEME = "gs";

    /**
     * The Hadoop, formed by combining system Hadoop config with properties defined in Flink config.
     */
    @Nullable private org.apache.hadoop.conf.Configuration hadoopConfig;

    /** The options used for GSFileSystem and RecoverableWriter. */
    @Nullable private GSFileSystemOptions fileSystemOptions;

    /**
     * Though it isn't documented as clearly as one might expect, the methods on this object are
     * threadsafe, so we can safely share a single instance among all file system instances.
     *
     * <p>Issue that discusses pending docs is here:
     * https://github.com/googleapis/google-cloud-java/issues/1238
     *
     * <p>StackOverflow discussion:
     * https://stackoverflow.com/questions/54516284/google-cloud-storage-java-client-pooling
     */
    @Nullable private Storage storage;

    /** Constructs the Google Storage file system factory. */
    public GSFileSystemFactory() {
        LOGGER.info("Creating GSFileSystemFactory");
    }

    /*

    */

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

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI fsUri, Configuration configuration) throws IOException {

        LOGGER.info("Creating GSFileSystem for uri {} with options {}", fsUri, fileSystemOptions);

        Preconditions.checkNotNull(fsUri);

        // create the Google Hadoop file system
        GoogleHadoopFileSystem googleHadoopFileSystem = new GoogleHadoopFileSystem();
        try {
            googleHadoopFileSystem.initialize(fsUri, hadoopConfig);
        } catch (IOException ex) {
            throw new IOException("Failed to initialize GoogleHadoopFileSystem", ex);
        }

        // create the file system
        return new GSFileSystem(googleHadoopFileSystem, storage, fileSystemOptions);

        /*
            @Override
        public void configure(Configuration flinkConfig) {
            Preconditions.checkNotNull(flinkConfig);

            ConfigUtils.ConfigContext configContext = new RuntimeConfigContext();

            // load Hadoop config
            this.hadoopConfig = ConfigUtils.getHadoopConfiguration(flinkConfig, configContext);
            LOGGER.info(
                    "Using Hadoop configuration {}", ConfigUtils.stringifyHadoopConfig(hadoopConfig));

            // construct file-system options
            this.fileSystemOptions = new GSFileSystemOptions(flinkConfig);
            LOGGER.info("Using file system options {}", fileSystemOptions);

            StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
            storageOptionsBuilder.setTransportOptions(getHttpTransportOptions(fileSystemOptions));
            storageOptionsBuilder.setRetrySettings(getRetrySettings(fileSystemOptions));

            // get storage credentials
            Optional<GoogleCredentials> credentials =
                    ConfigUtils.getStorageCredentials(hadoopConfig, configContext);
            credentials.ifPresent(storageOptionsBuilder::setCredentials);

            // override the GCS root URL only if overridden in the Hadoop config
            ConfigUtils.getGcsRootUrl(hadoopConfig).ifPresent(storageOptionsBuilder::setHost);

            this.storage = storageOptionsBuilder.build().getService();
        }
             */
    }

    @VisibleForTesting
    Storage getStorage() {
        return storage;
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
