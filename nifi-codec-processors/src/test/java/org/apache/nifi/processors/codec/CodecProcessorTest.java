/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.codec;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class CodecProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(CodecProcessor.class);
    }

    @Test
    public void testProcessor() {
        testRunner.setProperty(CodecProcessor.ADDITIONAL_CLASSPATH_RESOURCES," src/test/resources/SignGen.jar");
        testRunner.setProperty(CodecProcessor.CUSTOM_CLASS,"SimplePojo");
        testRunner.enqueue("Hello".getBytes(), Collections.singletonMap("filename", "test.txt"));
        testRunner.run();
        List<MockFlowFile> flowFilesList = testRunner.getFlowFilesForRelationship(CodecProcessor.ERROR);
        assertEquals(1,flowFilesList.size());

    }

}
