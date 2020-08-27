package com.jinfeng.flink;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import org.apache.flink.cassandra.shaded.com.google.common.reflect.TypeToken;

import java.util.LinkedHashSet;
import java.util.Set;

public class CassandraMappingCodec {
    private static class SetCodec<T> extends TypeCodec.AbstractCollectionCodec<T, Set<T>> {
        protected SetCodec(DataType.CollectionType cqlType, TypeToken<Set<T>> javaType, TypeCodec<T> eltCodec) {
            super(cqlType, javaType, eltCodec);
        }

        @Override
        protected Set<T> newInstance(int size) {
            return new LinkedHashSet<>(size);
        }

        /*
        private SetCodec(TypeCodec<T> eltCodec) {
            super(DataType.set(eltCodec.cqlType), TypeTokens.setOf(eltCodec.getJavaType()), eltCodec);
        }

        protected Set<T> newInstance(int size) {
            return new LinkedHashSet(size);
        }
        */
    }
}
