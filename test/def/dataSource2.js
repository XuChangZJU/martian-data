/**
 * Created by Administrator on 2016/6/1.
 */
"use strict";

const dataSources = {
    mysql : {
        type: "mysql",
        url: {
            host: "localhost",
            database: 'testorm',
            user: 'root',
            connectionLimit: 20,
        },
        settings: {
            disableUpdateAt: false,
            disableCreateAt: false,
            disableDeleteAt: false                                                  //  这三个值决定了在表中是否有相关动作的时间列。如果disableDeleteAt被设置为true，则删除是真的从数据库中删除
        }
    },
    mongodb: {
        type: "mongodb",
        url: "mongodb://localhost:27017/testOrm"
    }
};

module.exports = dataSources;
