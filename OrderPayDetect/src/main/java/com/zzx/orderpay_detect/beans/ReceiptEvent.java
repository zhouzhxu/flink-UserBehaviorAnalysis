package com.zzx.orderpay_detect.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: ReceiptEvent
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/13 18:29 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ReceiptEvent implements Serializable {

    private static final long serialVersionUID = 2715108575191927225L;

    /*
        交易码
     */
    private String txId;

    /*
        支付渠道
     */
    private String payChannel;

    /*
        时间戳
     */
    private Long timestamp;
}
