package com.zzx.loginfail_detect.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: LoginEvent
 * @author: wb-zcx696752
 * @description: 登录事件
 * @data: 2021/4/12 17:03 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class LoginEvent implements Serializable {

    private static final long serialVersionUID = 9010464034655537305L;

    /*
        用户id
     */
    private Long userId;

    /*
        登录ip
     */
    private String ip;

    /*
        登录状态
     */
    private String loginState;

    /*
        登录时间
     */
    private Long timestamp;
}
