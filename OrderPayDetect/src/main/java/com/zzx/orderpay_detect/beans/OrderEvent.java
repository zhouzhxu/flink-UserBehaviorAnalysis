package com.zzx.orderpay_detect.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: OrderEvent
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/13 15:06 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderEvent implements Serializable {

    private static final long serialVersionUID = 2807990650965612307L;

    /*
        订单id
     */
    private Long orderId;

    /*
        订单类型
     */
    private String eventType;

    /*
        交易码
     */
    private String txId;

    /*
        时间戳
     */
    private Long timestamp;

}
