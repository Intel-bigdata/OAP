package org.apache.parquet.io;

import org.apache.parquet.schema.*;

import java.util.ArrayList;
import java.util.List;


public class SpinachColumnIOCreatorVisitor implements TypeVisitor {
    private SMessageIO columnIO;
    private GroupColumnIO current;
    private List<PrimitiveColumnIO> leaves = new ArrayList<PrimitiveColumnIO>();
    private final MessageType requestedSchema;
    private final String createdBy;
    private int currentRequestedIndex;
    private Type currentRequestedType;
    private boolean strictTypeChecking;

    public SpinachColumnIOCreatorVisitor(MessageType requestedSchema, String createdBy, boolean strictTypeChecking) {
        this.requestedSchema = requestedSchema;
        this.createdBy = createdBy;
        this.strictTypeChecking = strictTypeChecking;
    }

    @Override
    public void visit(MessageType messageType) {
        columnIO = new SMessageIO(requestedSchema, createdBy);
        visitChildren(columnIO, messageType, requestedSchema);
        columnIO.setLevels();
        columnIO.setLeaves(leaves);
    }

    @Override
    public void visit(GroupType groupType) {
        if (currentRequestedType.isPrimitive()) {
            incompatibleSchema(groupType, currentRequestedType);
        }
        GroupColumnIO newIO = new GroupColumnIO(groupType, current, currentRequestedIndex);
        current.add(newIO);
        visitChildren(newIO, groupType, currentRequestedType.asGroupType());
    }

    private void visitChildren(GroupColumnIO newIO, GroupType groupType, GroupType requestedGroupType) {
        GroupColumnIO oldIO = current;
        current = newIO;
        for (Type type : groupType.getFields()) {
            // if the file schema does not contain the field it will just stay null
            if (requestedGroupType.containsField(type.getName())) {
                currentRequestedIndex = requestedGroupType.getFieldIndex(type.getName());
                currentRequestedType = requestedGroupType.getType(currentRequestedIndex);
                if (currentRequestedType.getRepetition().isMoreRestrictiveThan(type.getRepetition())) {
                    incompatibleSchema(type, currentRequestedType);
                }
                type.accept(this);
            }
        }
        current = oldIO;
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
        if (!currentRequestedType.isPrimitive() ||
                (this.strictTypeChecking && currentRequestedType.asPrimitiveType().getPrimitiveTypeName() != primitiveType.getPrimitiveTypeName())) {
            incompatibleSchema(primitiveType, currentRequestedType);
        }
        PrimitiveColumnIO newIO = new PrimitiveColumnIO(primitiveType, current, currentRequestedIndex, leaves.size());
        current.add(newIO);
        leaves.add(newIO);
    }

    private void incompatibleSchema(Type fileType, Type requestedType) {
        throw new ParquetDecodingException("The requested schema is not compatible with the file schema. incompatible types: " + requestedType + " != " + fileType);
    }

    public SMessageIO getColumnIO() {
        return columnIO;
    }
}
