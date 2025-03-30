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

package com.alibaba.fluss.fs.gs.writer;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.Path;
import com.alibaba.fluss.fs.gs.GSFileSystemOptions;
import com.alibaba.fluss.fs.gs.storage.GSBlobIdentifier;
import com.alibaba.fluss.fs.gs.storage.MockBlobStorage;
import com.alibaba.fluss.testutils.junit.parameterized.Parameter;
import com.alibaba.fluss.testutils.junit.parameterized.ParameterizedTestExtension;
import com.alibaba.fluss.testutils.junit.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link GSRecoverableWriter}. */
@ExtendWith(ParameterizedTestExtension.class)
class GSRecoverableWriterTest {

    @Parameter private long position = 16;

    @Parameter(value = 1)
    private boolean closed = false;

    @Parameter(value = 2)
    private int componentCount;

    @Parameters(name = "position={0}, closed={1}, componentCount={2}")
    private static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    // position 0, not closed, component count = 0
                    {0, false, 0},
                    // position 16, not closed, component count = 2
                    {16, false, 2},
                    // position 0, closed, component count = 0
                    {0, true, 0},
                    // position 16, closed, component count = 2
                    {16, true, 2},
                });
    }

    private GSFileSystemOptions options;

    private GSRecoverableWriter writer;

    private List<UUID> componentObjectIds;

    private GSResumeRecoverable resumeRecoverable;

    private GSCommitRecoverable commitRecoverable;

    private GSBlobIdentifier blobIdentifier;

    @BeforeEach
    void before() {
        MockBlobStorage storage = new MockBlobStorage();
        blobIdentifier = new GSBlobIdentifier("foo", "bar");

        Configuration flinkConfig = new Configuration();
        options = new GSFileSystemOptions(flinkConfig);
        writer = new GSRecoverableWriter(storage, options);

        componentObjectIds = new ArrayList<UUID>();
        for (int i = 0; i < componentCount; i++) {
            componentObjectIds.add(UUID.randomUUID());
        }

        resumeRecoverable =
                new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);
        commitRecoverable = new GSCommitRecoverable(blobIdentifier, componentObjectIds);
    }

    @TestTemplate
    void testRequiresCleanupOfRecoverableState() {
        assertThat(writer.requiresCleanupOfRecoverableState()).isFalse();
    }

    @TestTemplate
    void testSupportsResume() {
        assertThat(writer.supportsResume()).isTrue();
    }

    @TestTemplate
    void testOpen() throws IOException {
        Path path = new Path("gs://foo/bar");
        GSRecoverableFsDataOutputStream stream =
                (GSRecoverableFsDataOutputStream) writer.open(path);
        assertThat(stream).isNotNull();
    }

    @TestTemplate
    void testOpenWithEmptyBucketName() throws IOException {
        Path path = new Path("gs:///bar");

        assertThatThrownBy(() -> writer.open(path)).isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testOpenWithEmptyObjectName() throws IOException {
        Path path = new Path("gs://foo/");

        assertThatThrownBy(() -> writer.open(path)).isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testOpenWithMissingObjectName() throws IOException {
        Path path = new Path("gs://foo");

        assertThatThrownBy(() -> writer.open(path)).isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testCleanupRecoverableState() {
        assertThat(writer.cleanupRecoverableState(resumeRecoverable)).isTrue();
    }

    @TestTemplate
    void testRecover() throws IOException {
        GSRecoverableFsDataOutputStream stream =
                (GSRecoverableFsDataOutputStream) writer.recover(resumeRecoverable);
        assertThat(stream.getPos()).isEqualTo(position);
    }

    @TestTemplate
    void testRecoverForCommit() {
        GSRecoverableWriterCommitter committer =
                (GSRecoverableWriterCommitter) writer.recoverForCommit(commitRecoverable);
        assertThat(committer.options).isEqualTo(options);
        assertThat(committer.recoverable).isEqualTo(commitRecoverable);
    }

    @TestTemplate
    void testGetCommitRecoverableSerializer() {
        Object serializer = writer.getCommitRecoverableSerializer();
        assertThat(serializer.getClass()).isEqualTo(GSCommitRecoverableSerializer.class);
    }

    @TestTemplate
    void testGetResumeRecoverableSerializer() {
        Object serializer = writer.getResumeRecoverableSerializer();
        assertThat(serializer.getClass()).isEqualTo(GSResumeRecoverableSerializer.class);
    }
}
