package com.yzhou.blog.wordcount.checkpoint;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.IOException;

public class PartitioningResultKeyGroupReader<T> implements StateSnapshotKeyGroupReader {

    @Nonnull private final ElementReaderFunction<T> readerFunction;
    @Nonnull private final KeyGroupElementsConsumer<T> elementConsumer;

    public PartitioningResultKeyGroupReader(
            @Nonnull ElementReaderFunction<T> readerFunction,
            @Nonnull KeyGroupElementsConsumer<T> elementConsumer) {

        this.readerFunction = readerFunction;
        this.elementConsumer = elementConsumer;
    }

    @Override
    public void readMappingsInKeyGroup(@Nonnull DataInputView in, @Nonnegative int keyGroupId)
            throws IOException {
        int numElements = in.readInt();
        for (int i = 0; i < numElements; i++) {
            T element = readerFunction.readElement(in);
            elementConsumer.consume(element, keyGroupId);
        }
    }
}
