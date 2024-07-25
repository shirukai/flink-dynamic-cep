package org.apache.flink.cep.types;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 用来存储字段属性及值
 *
 * @author shirukai
 */
@Getter
@Setter
@JsonSerialize(using = FieldRow.Serializer.class)
public class FieldRow implements Serializable {
    private static final long serialVersionUID = 6022565744064408921L;
    private String[] fields;
    private Object[] values;
    /**
     * 存入元素个数
     */
    private int size = 0;

    public FieldRow() {

    }

    public static FieldRow of(int arity) {
        FieldRow row = new FieldRow();
        row.fields = new String[arity];
        row.values = new Object[arity];
        return row;
    }

    public FieldRow add(String name, Object value) {
        int i = indexOf(name);
        if (i == -1 && size < fields.length) {
            i = size;
            size++;
        } else {
            throw new IndexOutOfBoundsException("Length: " + fields.length + ",index: " + i);
        }
        fields[i] = name;
        values[i] = value;
        return this;
    }

    public Object get(String fieldName) {
        int index = indexOf(fieldName);
        if (index == -1) {
            return null;
        }
        return values[index];
    }

    public int indexOf(String fieldName) {
        if (fieldName == null) {
            for (int i = 0; i < fields.length; i++)
                if (fields[i] == null)
                    return i;
        } else {
            for (int i = 0; i < fields.length; i++)
                if (fieldName.equals(fields[i]))
                    return i;
        }
        return -1;
    }


    public void foreach(BiConsumer<String, Object> action) {
        for (int i = 0; i < fields.length; i++) {
            action.accept(fields[i], values[i]);
        }
    }

    public static class Serializer extends StdSerializer<FieldRow> {
        public Serializer() {
            super(FieldRow.class);
        }

        @Override
        public void serialize(FieldRow fieldRow, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            for (int i = 0; i < fieldRow.size; i++) {
                jsonGenerator.writeObjectField(fieldRow.fields[i], fieldRow.values[i]);
            }
            jsonGenerator.writeEndObject();
        }
    }


    @Override
    public String toString() {
        return "FieldRow{" +
                IntStream.range(0, fields.length).mapToObj(i -> fields[i] + "=" + values[i])
                        .collect(Collectors.joining(", ")) +
                '}';
    }
}
