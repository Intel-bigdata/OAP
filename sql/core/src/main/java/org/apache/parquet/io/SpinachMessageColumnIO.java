package org.apache.parquet.io;

import static org.apache.parquet.Preconditions.checkNotNull;

import java.util.List;

import org.apache.parquet.column.impl.SpinachColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.UnboundRecordFilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Visitor;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.recordlevel.FilteringRecordMaterializer;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicateBuilder;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.utils.Reflections;

public class SpinachMessageColumnIO extends MessageColumnIO {

    private List<PrimitiveColumnIO> leaves;

    private final boolean validating;

    private final String creatteBy;

    public SpinachMessageColumnIO(MessageColumnIO messageColumnIO) {
        super(messageColumnIO.getType(),
                (Boolean) Reflections.getFieldValue(messageColumnIO, "validating"),
                (String) Reflections.getFieldValue(messageColumnIO, "createdBy"));
        this.validating = (Boolean) Reflections.getFieldValue(messageColumnIO, "validating");
        this.creatteBy =  (String) Reflections.getFieldValue(messageColumnIO, "createdBy");
        this.leaves = messageColumnIO.getLeaves();
    }

    public <T> RecordReader<T> getRecordReader(final PageReadStore columns,
            final RecordMaterializer<T> recordMaterializer, final Filter filter) {
        checkNotNull(columns, "columns");
        checkNotNull(recordMaterializer, "recordMaterializer");
        checkNotNull(filter, "filter");

        if (leaves.isEmpty()) {
            return new EmptyRecordReader<T>(recordMaterializer);
        }

        return filter.accept(new Visitor<RecordReader<T>>() {
            @Override
            public RecordReader<T> visit(FilterPredicateCompat filterPredicateCompat) {

                FilterPredicate predicate = filterPredicateCompat.getFilterPredicate();
                IncrementallyUpdatedFilterPredicateBuilder builder =
                        new IncrementallyUpdatedFilterPredicateBuilder();
                IncrementallyUpdatedFilterPredicate streamingPredicate = builder.build(predicate);
                RecordMaterializer<T> filteringRecordMaterializer = new FilteringRecordMaterializer<T>(
                        recordMaterializer, leaves, builder.getValueInspectorsByColumn(), streamingPredicate);

                return new RecordReaderImplementation<T>(SpinachMessageColumnIO.this,
                        filteringRecordMaterializer, validating, new SpinachColumnReadStoreImpl(columns,
                                filteringRecordMaterializer.getRootConverter(), getType(),creatteBy));
            }

            @Override
            public RecordReader<T> visit(UnboundRecordFilterCompat unboundRecordFilterCompat) {
                throw new UnsupportedOperationException("Spinach not support UnboundRecordFilter feature.");
            }

            @Override
            public RecordReader<T> visit(NoOpFilter noOpFilter) {
                return new RecordReaderImplementation<T>(SpinachMessageColumnIO.this, recordMaterializer,
                        validating, new SpinachColumnReadStoreImpl(columns,
                                recordMaterializer.getRootConverter(), getType(),creatteBy));
            }
        });
    }

    public List<PrimitiveColumnIO> getLeaves() {
        return this.leaves;
    }

}
