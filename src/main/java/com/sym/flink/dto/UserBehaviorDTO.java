package com.sym.flink.dto;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhanglanchao
 * \* Date: 2021/4/9
 * \* Time: 12:54 下午
 * \* Description:
 * \
 *//** 用户行为数据结构 **/

public  class UserBehaviorDTO {
    public long userId;         // 用户ID
    public long itemId;         // 商品ID
    public int categoryId;      // 商品类目ID
    public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
    public long timestamp;      // 行为发生的时间戳，单位秒
}