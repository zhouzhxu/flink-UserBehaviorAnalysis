package com.zzx.market_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: ChannelPromotionCount
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/12 10:34 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ChannelPromotionCount implements Serializable {

    private static final long serialVersionUID = 2962344726525835842L;

    /*
        渠道
     */
    private String channel;

    /*
        用户行为
     */
    private String behavior;

    /*
        窗口结束时间
     */
    private String windowEnd;

    /*
        统计数量
     */
    private Long count;

}
