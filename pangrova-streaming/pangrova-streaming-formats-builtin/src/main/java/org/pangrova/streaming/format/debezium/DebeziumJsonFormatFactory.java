package org.pangrova.streaming.format.debezium;

import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.format.CdcFormatFactory;

/** Installs the Debezium JSON decoder under its SQL FORMAT identifier. */
public final class DebeziumJsonFormatFactory implements CdcFormatFactory {
    @Override
    public String identifier() {
        return DebeziumJsonConverter.FORMAT_ID;
    }

    @Override
    public CdcRecordConverter create() {
        return new DebeziumJsonConverter();
    }
}
