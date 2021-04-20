package com.zzx.loginfail_detect.beans;

import lombok.*;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 *
 * @ProjectName: UserBehaviorAnalysis
 * @ClassName: LoginFailWarning
 * @author: wb-zcx696752
 * @description: 登录失败信息
 * @data: 2021/4/12 17:06 PM
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class LoginFailWarning implements Serializable {

    private static final long serialVersionUID = -3696168582275562397L;

    /*
        用户id
     */
    private Long userId;

    /*
        第一次登录失败时间
     */
    private Long firstFailTime;

    /*
        最后一次登录失败时间
     */
    private Long lastFailTime;

    /*
        报警信息
     */
    private String warningMsg;

}
