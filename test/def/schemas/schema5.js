/**
 * Created by Administrator on 2016/6/2.
 */
"use strict";

const schema = {
    house: {
        source: "mysql",
        attributes: {
            buildAt:{
                type: "date",
                required : true,
                defaultValue : new Date()
            },
            status: {
                type: {
                    type: "enum",
                    values: ["verifying", "offline", "free", "inRent"]
                },
                required: true,
                defaultValue: "verifying"
            },
            houseInfo: {
                type: "ref",
                ref: "houseInfo"/*,
                required: true,
                localColumnName: "houseInfoId"                                         // 列名
                autoIndexed: false                                                      //  默认在外键列上会建索引，如果指定不建，这个域要加上false
                */
            },
            contract: {
                type: "ref",
                ref: "contract"
            }
        }
    },
    houseInfo: {
        source: "mongodb",
        attributes: {
            area:{
                type: "number",
                required : true
            },
            floor: {
                type: "int",
                required: true
            }
        }
    },
    contract: {
        source: "mongodb",
        attributes: {
            owner: {
                type: "ref",
                ref: "user",
                required: true
            },
            renter: {
                type: "ref",
                ref: "user",
                required: true
            },
            price: {
                type: "number",
                require: true
            }
        }
    },
    user: {
        source: 'mysql',
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
    }
};


module.exports = schema;
