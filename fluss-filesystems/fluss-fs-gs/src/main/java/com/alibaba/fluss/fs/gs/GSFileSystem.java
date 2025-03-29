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
import com.alibaba.fluss.fs.RecoverableWriter;
import com.alibaba.fluss.fs.gs.storage.GSBlobStorageImpl;
import com.alibaba.fluss.fs.gs.writer.GSRecoverableWriter;
import com.alibaba.fluss.fs.hdfs.HadoopFileSystem;
import com.alibaba.fluss.utils.Preconditions;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of the Fluss {@link FileSystem} interface for Google Cloud Storage. This class
 * implements the common behavior implemented directly by Fluss and delegates common calls to an
 * implementation of Hadoop's filesystem abstraction.
 */
public class GSFileSystem extends HadoopFileSystem {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSFileSystem.class);

    private final GSFileSystemOptions fileSystemOptions;

    private final Storage storage;

    GSFileSystem(
            GoogleHadoopFileSystem googleHadoopFileSystem,
            Storage storage,
            GSFileSystemOptions fileSystemOptions) {
        super(Preconditions.checkNotNull(googleHadoopFileSystem));
        this.fileSystemOptions = Preconditions.checkNotNull(fileSystemOptions);
        this.storage = Preconditions.checkNotNull(storage);
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        LOGGER.info("Creating GSRecoverableWriter with file-system options {}", fileSystemOptions);

        // create the GS blob storage wrapper
        GSBlobStorageImpl blobStorage = new GSBlobStorageImpl(storage);

        // construct the recoverable writer with the blob storage wrapper and the options
        return new GSRecoverableWriter(blobStorage, fileSystemOptions);
    }
}
