package com.cdd.models.utils;

public enum Modifier {
    LESS_THAN, GREATER_THAN, EQUAL, LESS_OR_EQUAL_THAN, GREATER_OR_EQUAL_THAN;

    public static Modifier fromString(String str) {
        if (str.equals(">")) {
            return GREATER_THAN;
        } else if (str.equals("<")) {
            return LESS_THAN;
        } else if (str.equals("=")) {
            return EQUAL;
        } else if (str.equals(">=")) {
            return GREATER_OR_EQUAL_THAN;
        } else if (str.equals("<=")) {
            return LESS_OR_EQUAL_THAN;
        } else {
            throw new IllegalArgumentException("Unknown modifier " + str);
        }
    }

    public String toString() {
        switch (this) {
            case GREATER_THAN:
                return ">";
            case LESS_THAN:
                return "<";
            case EQUAL:
                return "=";
            case GREATER_OR_EQUAL_THAN:
                return ">=";
            case LESS_OR_EQUAL_THAN:
                return "<=";
            default:
                throw new IllegalArgumentException();
        }
    }

    public boolean greater() {
        return this == GREATER_THAN || this == GREATER_OR_EQUAL_THAN;
    }

    public boolean less() {
        return this == LESS_THAN || this == LESS_OR_EQUAL_THAN;
    }

}
