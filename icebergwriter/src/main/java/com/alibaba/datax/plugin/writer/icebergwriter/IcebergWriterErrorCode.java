package com.alibaba.datax.plugin.writer.icebergwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum IcebergWriterErrorCode implements ErrorCode {
	
    ILLEGAL_VALUE("ILLEGAL_PARAMETER_VALUE","參數不合法"),
    ILLEGAL_ADDRESS("ILLEGAL_ADDRESS","不合法的寫入位置"),
    UNSUPPORTED_TYPE("UNSUPPORTED_TYPE","類型轉換異常，請注意查看 DataX 已經支持的數據庫類型以及數據庫版本."),
    UNEXCEPT_EXCEPTION("UNEXCEPT_EXCEPTION","未知異常");

    private final String code;

    private final String description;

    private IcebergWriterErrorCode(String code,String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
