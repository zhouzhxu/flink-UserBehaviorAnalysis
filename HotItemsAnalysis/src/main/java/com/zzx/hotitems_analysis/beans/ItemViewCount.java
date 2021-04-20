package com.zzx.hotitems_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @author: wb-zcx696752
 * @description: 商品计数
 * @data: 2021/4/2 15:31 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ItemViewCount implements Serializable {

    private static final long serialVersionUID = -640016632958144765L;

    /*
        商品 id
     */
    private Long itemId;

    /*
        window 结束时间
     */
    private Long windowEnd;

    /*
        商品数量
     */
    private Long count;

}
