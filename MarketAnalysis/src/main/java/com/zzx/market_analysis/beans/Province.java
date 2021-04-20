package com.zzx.market_analysis.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: Province
 * @author: wb-zcx696752
 * @description:
 * @data: 2021/4/12 15:21 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Province implements Serializable {

    private static final long serialVersionUID = -9080467038988760812L;

    private String province;

    private String city;
}
