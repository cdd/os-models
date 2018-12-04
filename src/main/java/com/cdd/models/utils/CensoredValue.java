package com.cdd.models.utils;

import java.io.Serializable;

public class CensoredValue implements Serializable{
    final double value;
    final Modifier modifier;

    public CensoredValue(double value, Modifier modifier) {
        this.value = value;
        this.modifier = modifier;
    }

    public double getValue() {
        return value;
    }

    public Modifier getModifier() {
        return modifier;
    }
}
