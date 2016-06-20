/**
 * Created by Administrator on 2016/6/19.
 */
"use strict";

const schema = {
    house:{ //房屋表
        source: "mysql",
        attributes: {
            owner:{
                type: "ref",
                ref: "user",
                required: true/*,
                 localColumnName: "ownerId"                                         // 列名
                 autoIndexed: false                                                      //  默认在外键列上会建索引，如果指定不建，这个域要加上false
                 */
            },
            key:{
                type: "ref",
                ref: "key",
                required: true,
                localColumnName: "masterKeyId" ,                                        // 列名
                autoIndexed: true                                                 //  默认在外键列上会建索引，如果指定不建，这个域要加上false
            },
            status: {
                type: {
                    type: "enum",
                    values: ["unfinished", "notThrough","verifying", "offline", "free", "inRent"]
                },
                required: true,
                defaultValue: "verifying"
            },
            houseInfo:{
                type: "ref",
                ref: "houseInfo",
                required: true
            }
        }
    },
    houseInfo: { //房屋信息表
        source: "mongodb",
        attributes: {
            status: {
                type:"string"
            },
            cer:{
                type:"object"
            },
            prop:{
                type:"object"
            },
            demand:{
                type:"object"
            }
        }
    },
    consult: { //咨询表
        source: "mongodb",
        attributes: {
            house:{
                type:"ref",
                ref:"house",
                required: true
            },
            tenant :{
                type:"ref",
                ref:"user",
                required: true
            },
            details :{
                type:"object"
            }
        }
    },
    key:{
        source: "mongodb",
        attributes: {
            owner:{
                type: "ref",
                ref: "user",
                required: true
            },
            name:{
                type: "string",
                size: 15,
                required: true
            }
        }
    },
    user: {
        source: "mongodb",
        attributes: {
            name: {
                type: {
                    type: "string",
                    size: 12
                },
                required: true
            },
            age: {
                type: "int"
            }
        }
    },
    lease: { //租约表
        source: "mysql",
        attributes: {
            house:{
                type:"ref",
                ref:"house",
                required: true
            },
            landlord :{
                type:"ref",
                ref:"user",
                required: true
            },
            tenant :{
                type:"ref",
                ref:"user",
                required: true
            },
            leaseBeginTime:{
                type:"date",
                required: true
            },
            leaseEndTime:{
                type:"date",
                required: true
            }
        }
    },
    orderForm: { //订单表
        source: "mysql",
        attributes: {
            type:{
                type:"int",
                required: true
            },
            house:{
                type:"ref",
                ref:"house",
                required: true
            },
            user :{
                type:"ref",
                ref:"user",
                required: true
            }
        }
    },
    leaseAction: { // 租约操作表
        source: "mysql",
        attributes: {
            lease:{
                type:"ref",
                ref:"lease",
                required: true
            }
        }
    }

};

module.exports = schema;
