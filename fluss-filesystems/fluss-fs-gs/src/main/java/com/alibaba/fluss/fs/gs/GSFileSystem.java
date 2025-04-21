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

import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.gs.token.GSTokenProvider;
import com.alibaba.fluss.fs.hdfs.HadoopFileSystem;

import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.auth.GcsDelegationTokens;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Implementation of the Fluss {@link FileSystem} interface for Google Cloud Storage. This class
 * implements the common behavior implemented directly by Fluss and delegates common calls to an
 * implementation of Hadoop's filesystem abstraction.
 */
public class GSFileSystem extends HadoopFileSystem {

    private final String scheme;
    private final Configuration conf;

    private volatile GSTokenProvider gsTokenProvider;
    /**
     * Creates a GSFileSystem based on the given Hadoop Google Cloud Storage file system. The given
     * Hadoop file system object is expected to be initialized already.
     *
     * <p>This constructor additionally configures the entropy injection for the file system.
     *
     * @param hadoopGSFileSystem The Hadoop FileSystem that will be used under the hood.
     */
    public GSFileSystem(String scheme, GoogleHadoopFileSystem hadoopGSFileSystem, Configuration conf) {
        super(hadoopGSFileSystem);
        this.scheme = scheme;
        this.conf = conf;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        if(gsTokenProvider==null) {
            synchronized (this) {
                if(gsTokenProvider==null) {
                    gsTokenProvider = new GSTokenProvider(scheme, conf);
                }
            }
        }

        return gsTokenProvider.obtainSecurityToken();
    }
}
