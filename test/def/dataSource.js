/**
 * Created by Administrator on 2016/6/1.
 */
"use strict";

const dataSources = {
    mysql : {
        type: "mysql",
        url: "mysql://root@localhost:3306/testorm",
        settings: {
            "connection.pool": true
        }
    },
    mongodb: {
        type: "mongodb",
        url: "mongodb://localhost:27017/testOrm"
    }
};

module.exports = dataSources;
