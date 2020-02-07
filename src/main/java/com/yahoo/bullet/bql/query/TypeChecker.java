package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

// TODO add context of errors
public class TypeChecker {
    public static Optional<List<BulletError>> validateFieldType(Type type, boolean hasIndexOrKey, boolean hasSubKey) {
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        if (hasSubKey) {
            return isComplex(type) ?  Optional.empty() : makeError("Cannot take subkey of field with type: " + type);
        }
        if (hasIndexOrKey) {
            return isCollection(type) ? Optional.empty() : makeError("Cannot take index or key of field with type: " + type);
        }
        return Optional.empty();
    }

    public static Type getFieldType(Type type, boolean hasIndexOrKey, boolean hasSubKey) {
        if (hasSubKey) {
            return type.getSubType().getSubType();
        }
        if (hasIndexOrKey) {
            return type.getSubType();
        }
        return type;
    }

    public static Optional<List<BulletError>> validateListTypes(List<Type> types) {
        Set<Type> typeSet = EnumSet.copyOf(types);
        if (typeSet.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        typeSet.remove(Type.NULL);
        // TODO currently can't support empty list
        if (typeSet.isEmpty()) {
            // TODO list would be all nulls in this scenario. maybe don't support null elements and this won't be a concern..
            return makeError("Can't figure out type of list");
        }
        if (typeSet.size() > 1) {
            return makeError("List consists of multiple types: " + types);
        }
        Type type = typeSet.iterator().next();
        if (!Type.isPrimitive(type) && !Type.isPrimitiveMap(type)) {
            return makeError("List's subtype cannot be: " + type);
        }
        return Optional.empty();
    }

    public static Type getListType(List<Type> types) {
        Set<Type> typeSet = EnumSet.copyOf(types);
        typeSet.remove(Type.NULL);
        // Assume non-empty list
        Type subType = typeSet.iterator().next();
        return Type.LISTS.stream().filter(t -> subType.equals(t.getSubType())).findFirst().orElse(Type.UNKNOWN);
    }

    public static Optional<List<BulletError>> validateUnaryType(Type type, Operation op) {
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        switch (op) {
            case NOT:
                return Type.isNumeric(type) || type == Type.BOOLEAN ? Optional.empty() : makeError("Cannot have NOT with type: " + type);
            case SIZE_OF:
                return isCollection(type) || type == Type.STRING ? Optional.empty() : makeError("Cannot have SIZEOF with type: " + type);
            case IS_NULL:
            case IS_NOT_NULL:
                return Optional.empty();
        }
        // Can't reach normally
        return makeError("This is not a unary operation: " + op);
    }

    public static Type getUnaryType(Operation op) {
        switch (op) {
            case NOT:
            case IS_NULL:
            case IS_NOT_NULL:
                return Type.BOOLEAN;
            case SIZE_OF:
                return Type.INTEGER;
        }
        // Can't reach normally
        return Type.UNKNOWN;
    }

    public static Optional<List<BulletError>> validateNAryType(List<Type> types, Operation op) {
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        // only IF supported at the moment
        switch (op) {
            case IF:
                if (types.get(0) != Type.BOOLEAN) {
                    return makeError("Type of first argument must be BOOLEAN. Actual type: " + types.get(0));
                }
                if (types.get(1) != types.get(2)) {
                    return makeError("Types of second and third arguments must match. Actual types: " + types.get(1) + ", " + types.get(2));
                }
                return Optional.empty();
        }
        // Can't reach normally
        return makeError("This is not a supported n-ary operation: " + op);
    }

    public static Type getNAryType(List<Type> types, Operation op) {
        // only IF supported at the moment
        switch (op) {
            case IF:
                return types.get(1);
        }
        // Can't reach normally
        return Type.UNKNOWN;
    }

    public static Optional<List<BulletError>> validateAggregateType(Type type) {
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        if (!Type.isNumeric(type)) {
            return makeError("Argument's type must be numeric. Actual type given: " + type);
        }
        return Optional.empty();
    }

    public static Type getAggregateType(Type type, GroupOperation.GroupOperationType op) {
        switch (op) {
            case SUM:
            case MIN:
            case MAX:
                return type;
            case AVG:
                return Type.DOUBLE;
        }
        // Can't reach normally
        return Type.UNKNOWN;
    }

    public static Optional<List<BulletError>> validateCountDistinctType(List<Type> types) {
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        /*
        if (types.stream().anyMatch(type -> !Type.isPrimitive(type))) {
            return makeError("All types in count distinct must be primitive. given types: " + types);
        }
        */
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateDistributionType(Type type) {
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        if (!Type.isNumeric(type)) {
            return makeError("gotta be numeric. given: " + type);
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateTopKType(List<Type> types) {
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        /*
        if (types.stream().anyMatch(type -> !Type.isPrimitive(type))) {
            return makeError("All types in top k must be primitive. given types: " + types);
        }
        */
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateCastType(Type type, Type castType) {
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        /*
        if (!Type.canCast(castType, type)) {
            return makeError("Can't cast from " + type + " to " + castType);
        }
        */
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateBinaryType(Type leftType, Type rightType, Operation op) {
        if (Type.isUnknown(leftType) || Type.isUnknown(rightType)) {
            return unknownError();
        }
        switch (op) {
            case ADD:
            case SUB:
            case MUL:
            case DIV:
            // TODO add primitive-to-list comparison?
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
                return Type.isNumeric(leftType) && Type.isNumeric(rightType) ? Optional.empty() : makeError("Left and right operand need to be numbers. given types: " + leftType + ", " + rightType);
            case EQUALS:
                // TODO add primitive-to-list equals?
            case NOT_EQUALS:
                // TODO add primitive-to-list not equals?
                return leftType == rightType ? Optional.empty() : makeError("Left and right operand must have the same type. given types: " + leftType + ", " + rightType);
            case REGEX_LIKE:
                return leftType == Type.STRING && rightType == Type.STRING ? Optional.empty() : makeError("Both arguments need to be strings. given types: " + leftType + ", " + rightType);
            case SIZE_IS:
                return (isCollection(leftType) || leftType == Type.STRING) && Type.isNumeric(rightType) ? Optional.empty() : makeError("First argument needs to be a string/list/map and second argument needs to be a number. given types: " + leftType + ", " + rightType);
            case CONTAINS_KEY:
                return Type.isMap(leftType) && rightType == Type.STRING ? Optional.empty() : makeError("First argument needs to be a map and second argument needs to be a string. given types: " + leftType + ", " + rightType);
            case CONTAINS_VALUE:
                return isCollection(leftType) && leftType.getSubType() == rightType ? Optional.empty() : makeError("First argument needs to be a collection and second argument must match the first argument's subtype. given types: " + leftType + ", " + rightType);
            case AND:
            case OR:
            case XOR:
                return leftType == Type.BOOLEAN && rightType == Type.BOOLEAN ? Optional.empty() : makeError("Arguments need to be boolean. given types: " + leftType + ", " + rightType);
            case FILTER:
                return Type.isList(leftType) && rightType == Type.BOOLEAN_LIST ? Optional.empty() : makeError("First argument needs to be a list and second argument must be a boolean list. given types:" + leftType + ", " + rightType);
        }
        // Can't reach normally
        return makeError("This is not a binary operation: " + op);
    }

    public static Type getBinaryType(Type leftType, Type rightType, Operation op) {
        switch (op) {
            case ADD:
            case SUB:
            case MUL:
            case DIV:
                if (leftType == Type.DOUBLE || rightType == Type.DOUBLE) {
                    return Type.DOUBLE;
                } else if (leftType == Type.FLOAT || rightType == Type.FLOAT) {
                    return Type.FLOAT;
                } else if (leftType == Type.LONG || rightType == Type.LONG) {
                    return Type.LONG;
                } else {
                    return Type.INTEGER;
                }
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
            case REGEX_LIKE:
            case SIZE_IS:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
            case AND:
            case OR:
            case XOR:
                return Type.BOOLEAN;
            case FILTER:
                return leftType;
        }
        // Can't reach normally
        return Type.UNKNOWN;
    }

    // Made this a static method and not a constant because IDE was complaining that making a static final optional is semantically inappropriate or something
    private static Optional<List<BulletError>> unknownError() {
        return Optional.of(Collections.emptyList());
    }

    private static Optional<List<BulletError>> makeError(String message) {
        return Optional.of(Collections.singletonList(new BulletError(message, null)));
    }

    private static boolean isComplex(Type type) {
        return Type.isComplexList(type) || Type.isComplexMap(type);
    }

    private static boolean isCollection(Type type) {
        return Type.isList(type) || Type.isMap(type);
    }
}
