package com.tml;

import com.tml.msg.CommonMsg;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class TokenMap implements MapFunction<String, CommonMsg> {
    @Override
    public CommonMsg map(String value) throws Exception {
        CommonMsg msg = new CommonMsg();
        if (StringUtils.isBlank(value)) {
            return msg;
        }
        try{
            String[] split = value.split("\\|");
            msg.setId(split[0]);
            msg.setMsg(split[1]);
            msg.setTime(Long.valueOf(split[2]));
        }catch (Exception e){
            e.printStackTrace();
        }

        return msg;
    }
}
