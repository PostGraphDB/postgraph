/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.age.jdbc.base;

import java.util.Queue;
import org.apache.age.jdbc.AgtypeUnrecognizedList;
import org.apache.age.jdbc.AgtypeUnrecognizedMap;
import org.apache.age.jdbc.antlr4.AgtypeBaseListener;
import org.apache.age.jdbc.antlr4.AgtypeParser.AgTypeContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.ArrayValueContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.FalseBooleanContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.FloatValueContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.IntegerValueContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.NullValueContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.ObjectValueContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.PairContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.StringValueContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.TrueBooleanContext;
import org.apache.age.jdbc.antlr4.AgtypeParser.TypeAnnotationContext;
import org.apache.age.jdbc.base.type.AgtypeList;
import org.apache.age.jdbc.base.type.AgtypeListImpl;
import org.apache.age.jdbc.base.type.AgtypeMap;
import org.apache.age.jdbc.base.type.AgtypeMapImpl;
import org.apache.age.jdbc.base.type.AgtypeObject;
import org.apache.age.jdbc.base.type.UnrecognizedObject;
import org.apache.commons.text.StringEscapeUtils;

public class AgtypeListener extends AgtypeBaseListener {

    // Will have List or Map
    private final Queue<AgtypeObject> objectQueue = new Queue<>();
    private final Queue<String> annotationMap = new Queue<>();
    Object rootObject;
    Object lastValue;
    boolean lastValueUndefined = true;

    private long objectQueueLength = 0;

    private void pushObjectQueue(AgtypeObject o) {
        objectQueueLength++;
        this.objectQueue.push(o);
    }

    private AgtypeObject popObjectQueue() {
        objectQueueLength--;
        return objectQueue.pop();
    }

    private AgtypeObject peekObjectQueue() {
        return objectQueue.peek();
    }

    private void mergeObjectIfTargetIsArray() {
        if (objectQueueLength >= 2) {
            AgtypeObject firstObject = popObjectQueue();
            AgtypeObject secondObject = popObjectQueue();
            if (secondObject instanceof AgtypeListImpl) {
                ((AgtypeListImpl) secondObject).add(firstObject);
                pushObjectQueue(secondObject);
            } else {
                pushObjectQueue(secondObject);
                pushObjectQueue(firstObject);
            }
        }
    }

    private void mergeObjectIfTargetIsMap(String key, Object value) {
        AgtypeMapImpl agtypeMap = (AgtypeMapImpl) peekObjectQueue();
        agtypeMap.put(key, value);
    }

    private void addObjectValue() {
        if (objectQueueLength != 0) {
            AgtypeObject currentObject = peekObjectQueue();
            if (currentObject instanceof AgtypeListImpl) {
                ((AgtypeListImpl) currentObject).add(this.lastValue);
                lastValueUndefined = true;
                return;
            }
        }
        lastValueUndefined = false;
    }

    @Override
    public void exitStringValue(StringValueContext ctx) {
        this.lastValue = identString(ctx.STRING().getText());
        addObjectValue();
    }

    @Override
    public void exitIntegerValue(IntegerValueContext ctx) {
        this.lastValue = Long.parseLong(ctx.INTEGER().getText());
        addObjectValue();
    }

    @Override
    public void exitFloatValue(FloatValueContext ctx) {
        this.lastValue = Double.parseDouble(ctx.floatLiteral().getText());
        addObjectValue();
    }

    @Override
    public void exitTrueBoolean(TrueBooleanContext ctx) {
        this.lastValue = true;
        addObjectValue();
    }

    @Override
    public void exitFalseBoolean(FalseBooleanContext ctx) {
        this.lastValue = false;
        addObjectValue();
    }

    @Override
    public void exitNullValue(NullValueContext ctx) {
        this.lastValue = null;
        addObjectValue();
    }

    @Override
    public void enterObjectValue(ObjectValueContext ctx) {
        AgtypeMap agtypeMap = new AgtypeUnrecognizedMap();
        pushObjectQueue(agtypeMap);
    }

    @Override
    public void exitObjectValue(ObjectValueContext ctx) {
        mergeObjectIfTargetIsArray();
    }

    @Override
    public void enterArrayValue(ArrayValueContext ctx) {
        AgtypeList agtypeList = new AgtypeUnrecognizedList();
        pushObjectQueue(agtypeList);
    }

    @Override
    public void exitArrayValue(ArrayValueContext ctx) {
        mergeObjectIfTargetIsArray();
    }

    @Override
    public void exitPair(PairContext ctx) {
        String name = identString(ctx.STRING().getText());
        if (!lastValueUndefined) {
            mergeObjectIfTargetIsMap(name, this.lastValue);
            lastValueUndefined = true;
        } else {
            Object lastValue = popObjectQueue();
            Object currentHeaderObject = peekObjectQueue();
            if (currentHeaderObject instanceof AgtypeListImpl) {
                ((AgtypeListImpl) currentHeaderObject).add(lastValue);
            } else {
                mergeObjectIfTargetIsMap(name, lastValue);
            }
        }
    }

    @Override
    public void exitAgType(AgTypeContext ctx) {
        if (objectQueue.empty()) {
            this.rootObject = this.lastValue;
            return;
        }
        this.rootObject = popObjectQueue();
    }

    @Override
    public void enterTypeAnnotation(TypeAnnotationContext ctx) {
        annotationMap.push(ctx.IDENT().getText());
    }

    @Override
    public void exitTypeAnnotation(TypeAnnotationContext ctx) {
        String annotation = annotationMap.pop();
        Object currentObject = peekObjectQueue();
        if (currentObject instanceof UnrecognizedObject) {
            ((UnrecognizedObject) currentObject).setAnnotation(annotation);
        }
    }

    private String identString(String quotesString) {
        return StringEscapeUtils.unescapeJson(quotesString.substring(1, quotesString.length() - 1));
    }

    public Object getOutput() {
        return this.rootObject;
    }
}
