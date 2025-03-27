package com.tml.msg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 定义通用的消息POJO
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommonMsg {

    private String id;

    private String msg;

    private Long time;


}
