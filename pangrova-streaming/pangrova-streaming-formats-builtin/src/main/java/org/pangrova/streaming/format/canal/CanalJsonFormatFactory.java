package org.pangrova.streaming.format.canal;

import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.format.CdcFormatFactory;

/** Installs the Canal JSON decoder under its SQL FORMAT identifier. */
public final class CanalJsonFormatFactory implements CdcFormatFactory {
    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public String identifier() {
        return CanalJsonConverter.FORMAT_ID;
    }

    @Override
    public CdcRecordConverter create() {
        return new CanalJsonConverter();
    }
}
