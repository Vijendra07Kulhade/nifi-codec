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

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"codec"})
@CapabilityDescription("Encoder Decoder processor")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CodecProcessor extends AbstractProcessor {

    public static final PropertyDescriptor ADDITIONAL_CLASSPATH_RESOURCES = new PropertyDescriptor.Builder()
            .name("Additional Classpath Resources")
            .displayName("Additional Classpath")
            .description("A comma-separated list of paths to files and/or directories that will be added to the classpath and used for loading native libraries. " +
                    "When specifying a directory, all files with in the directory will be added to the classpath, but further sub-directories will not be included.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamicallyModifiesClasspath(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor CUSTOM_CLASS = new PropertyDescriptor
            .Builder().name("Custom Class Name")
            .displayName("Custom Class Name")
            .description("Custom Class Name too load")
            .required(true)
            .defaultValue("standard_feed")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship")
            .build();
    static final Relationship ERROR = new Relationship.Builder()
            .name("error")
            .description("Error relationship")
            .build();
    static final Relationship ORIGINAL = new Relationship.Builder()
            .name("Original")
            .description("Original flow-file relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ADDITIONAL_CLASSPATH_RESOURCES);
        descriptors.add(CUSTOM_CLASS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(ERROR);
        relationships.add(ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        String classpath = context.getProperty(ADDITIONAL_CLASSPATH_RESOURCES).evaluateAttributeExpressions(flowFile).getValue();
        String customClassName = context.getProperty(CUSTOM_CLASS).evaluateAttributeExpressions(flowFile).getValue();
        getLogger().log(LogLevel.INFO,String.format("found classpath %s",classpath));
        getLogger().log(LogLevel.INFO,String.format("found customClassName %s",customClassName));
        try {
            Class<?> cls = Thread.currentThread().getContextClassLoader().loadClass(customClassName);
            getLogger().log(LogLevel.INFO,String.format("Loaded customClassName %s",cls.getCanonicalName()));
            session.transfer(flowFile, ORIGINAL);
        }catch (ClassNotFoundException c){
            getLogger().error(String.format("Error while loading class %s(%s). Is it valid?", customClassName,classpath), c);
            session.transfer(flowFile, ERROR);
        }
    }
}
