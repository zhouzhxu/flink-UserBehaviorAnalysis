package com.zzx.networkflow_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/7 11:06 PM
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ApacheLogEvent implements Serializable {

    private static final long serialVersionUID = 5163142090876411074L;

    /*
        id地址
     */
    private String ip;

    /*
        用户id
     */
    private String userId;

    /*
        时间
     */
    private Long timestamp;

    /*
        请求方式
     */
    private String method;

    /*
        请求地址
     */
    private String url;

}
