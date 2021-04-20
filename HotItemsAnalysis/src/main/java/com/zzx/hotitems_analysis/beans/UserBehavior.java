package com.zzx.hotitems_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 用户行为
 * @data: 2021/4/2 15:24 PM
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UserBehavior implements Serializable {

    private static final long serialVersionUID = -779736119060048218L;

    /*
        用户id
     */
    private Long userId;

    /*
        商品id
     */
    private Long itemId;

    /*
        类别id
     */
    private Integer categoryId;

    /*
        用户行为
     */
    private String behavior;

    /*
        时间戳
     */
    private Long timestamp;

}
