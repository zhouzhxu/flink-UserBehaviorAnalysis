package com.zzx.networkflow_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/7 11:09 PM
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class PageViewCount implements Serializable {

    private static final long serialVersionUID = -676465574207783160L;

    /*
        请求地址
     */
    private String url;

    /*
        窗口结束时间
     */
    private Long windowEnd;

    /*
        窗口内统计数量
     */
    private Long count;
    
}
