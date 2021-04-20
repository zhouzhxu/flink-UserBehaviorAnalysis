package com.zzx.market_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: AdCountViewByProvince
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/12 15:08 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class AdCountViewByProvince implements Serializable {

    private static final long serialVersionUID = -6150135468863143420L;

    /*
        省份
     */
    private String province;

    /*
        窗口结束时间
     */
    private String windowEnd;

    /*
        窗口内统计数量
     */
    private Long count;

}
