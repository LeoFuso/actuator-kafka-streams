package io.github.leofuso.autoconfigure.actuator.kafka.streams.utils;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.springframework.util.unit.DataSize;

/**
 * Static utilities for serialization and deserialization using <a
 * href="https://docs.oracle.com/javase/8/docs/technotes/guides/serialization/" target="_blank">Java Object
 * Serialization</a>.
 *
 * <p><strong>WARNING</strong>: These utilities should be used with caution. See
 * <a href="https://www.oracle.com/java/technologies/javase/seccodeguide.html#8" target="_blank">Secure Coding
 * Guidelines for the Java Programming Language</a> for details.
 */
public abstract class SerializationUtils {

    /**
     * Serialize the given object to a byte array.
     *
     * @param object the object to serialize
     * @return an array of bytes representing the object. Returns an empty array if the object reference is null.
     */
    public static byte[] serialize(Object object) {

        /* We don't have to worry about null references */
        if (object == null) {
            return new byte[0];
        }

        final DataSize buffer = DataSize.ofKilobytes(1L);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream((int) buffer.toBytes());
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Serialization failure. Object of type [" + object.getClass() + "]", ex);
        }
    }

    /**
     * Deserialize the byte array into an object.
     * <p><strong>WARNING</strong>: This utility allows arbitrary code to be run.
     * <p>
     * Java Object Serialization is known for being the source of many Remote Code Execution (RCE) vulnerabilities.
     *
     * @param bytes a serialized object
     * @param <T>   the type to cast the deserialized object to.
     * @return the result of deserializing the bytes
     */

    @Nullable
    public static <T> T deserialize(byte[] bytes) {

        if (bytes == null) {
            return null;
        }

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            @SuppressWarnings("unchecked")
            final T typedObject = (T) objectInputStream.readObject();
            return typedObject;
        } catch (IOException | ClassNotFoundException ex) {
            throw new IllegalArgumentException("Deserialization failure.", ex);
        }
    }

}