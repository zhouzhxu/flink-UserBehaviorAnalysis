package com.zzx.market_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: AdClickEvent
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/12 15:02 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class AdClickEvent implements Serializable {

    private static final long serialVersionUID = 4614640823237602998L;

    /*
        用户id
     */
    private Long userId;

    /*
        广告id
     */
    private Long adId;

    /*
        省份
     */
    private String province;

    /*
        城市
     */
    private String city;

    /*
        时间
     */
    private Long timestamp;
}
