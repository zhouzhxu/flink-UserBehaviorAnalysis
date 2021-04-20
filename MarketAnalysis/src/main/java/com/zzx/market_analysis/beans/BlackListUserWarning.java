package com.zzx.market_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: BlackListUserWarning
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/12 16:22 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class BlackListUserWarning implements Serializable {

    private static final long serialVersionUID = -2342265799787125540L;

    private Long userId;

    private Long adId;

    private String warningMsg;
}
