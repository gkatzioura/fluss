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

import com.alibaba.fluss.fs.gs.storage.GSBlobIdentifier;
import com.alibaba.fluss.testutils.junit.parameterized.Parameter;
import com.alibaba.fluss.testutils.junit.parameterized.ParameterizedTestExtension;
import com.alibaba.fluss.testutils.junit.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link GSResumeRecoverable}. */
@ExtendWith(ParameterizedTestExtension.class)
class GSResumeRecoverableTest {

    @Parameter private int position;

    @Parameter(value = 1)
    private boolean closed;

    @Parameter(value = 2)
    private List<UUID> componentObjectIds;

    @Parameter(value = 3)
    private String temporaryBucketName;

    @Parameters(name = "position={0}, closed={1}, componentObjectIds={2}, temporaryBucketName={3}")
    private static Collection<Object[]> data() {

        ArrayList<UUID> emptyComponentObjectIds = new ArrayList<>();
        ArrayList<UUID> populatedComponentObjectIds = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            populatedComponentObjectIds.add(UUID.randomUUID());
        }
        GSBlobIdentifier blobIdentifier = new GSBlobIdentifier("foo", "bar");

        return Arrays.asList(
                new Object[][] {
                    //  position=0, closed, no component ids, no explicit temporary bucket name
                    {0, true, emptyComponentObjectIds, null},
                    //  position=1024, not closed, no component ids, no explicit temporary bucket
                    // name
                    {1024, false, emptyComponentObjectIds, null},
                    //  position=0, closed, populated component ids, no explicit temporary bucket
                    // name
                    {0, true, populatedComponentObjectIds, null},
                    //  position=1024, not closed, populated component ids, no explicit temporary
                    // bucket name
                    {1024, false, populatedComponentObjectIds, null},
                    //  position=0, closed, populated component ids, explicit temporary bucket name
                    {0, true, populatedComponentObjectIds, "temporary-bucket"},
                    //  position=1024, not closed, populated component ids, explicit temporary
                    // bucket name
                    {1024, false, populatedComponentObjectIds, "temporary-bucket"},
                });
    }

    private GSBlobIdentifier blobIdentifier;

    @BeforeEach
    void before() {
        blobIdentifier = new GSBlobIdentifier("foo", "bar");
    }

    @TestTemplate
    void shouldConstructProperly() {
        GSResumeRecoverable resumeRecoverable =
                new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);
        assertThat(resumeRecoverable.finalBlobIdentifier).isEqualTo(blobIdentifier);
        assertThat(resumeRecoverable.position).isEqualTo(position);
        assertThat(resumeRecoverable.closed).isEqualTo(closed);
        assertThat(resumeRecoverable.componentObjectIds).isEqualTo(componentObjectIds);
    }

    /** Ensure that the list of component object ids cannot be added to. */
    @TestTemplate
    void shouldNotAddComponentId() {
        GSResumeRecoverable resumeRecoverable =
                new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);

        assertThatThrownBy(() -> resumeRecoverable.componentObjectIds.add(UUID.randomUUID()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** Ensure that component object ids can't be updated. */
    @TestTemplate
    void shouldNotModifyComponentId() {
        GSResumeRecoverable resumeRecoverable =
                new GSResumeRecoverable(blobIdentifier, componentObjectIds, position, closed);

        assertThatThrownBy(() -> resumeRecoverable.componentObjectIds.set(0, UUID.randomUUID()))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
