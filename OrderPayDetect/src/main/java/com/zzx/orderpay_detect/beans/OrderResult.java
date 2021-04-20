package com.zzx.orderpay_detect.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: OrderResult
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/13 15:17 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderResult implements Serializable {

    private static final long serialVersionUID = 3810620479693107179L;

    /*
        订单id
     */
    private Long orderId;

    /*
        结果状态
     */
    private String resultState;
}
