package com.zzx.market_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: MarketingUserBehavior
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/12 10:30 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MarketingUserBehavior implements Serializable {

    private static final long serialVersionUID = -7661925067755785450L;

    /*
        用户 id
     */
    private Long userId;

    /*
        用户行为
     */
    private String behavior;

    /*
        渠道
     */
    private String channel;

    /*
        时间
     */
    private Long timestamp;
}
