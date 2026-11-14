package org.pangrova.streaming.format.maxwell;

import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.format.CdcFormatFactory;

/** Installs the Maxwell JSON decoder under its SQL FORMAT identifier. */
public final class MaxwellJsonFormatFactory implements CdcFormatFactory {
    @Override
    public String identifier() {
        return MaxwellJsonConverter.FORMAT_ID;
    }

    @Override
    public CdcRecordConverter create() {
        return new MaxwellJsonConverter();
    }
}
