/**
 * Created by Administrator on 2016/6/21.
 */
"use strict";

let sha1 = require('sha1');
const remoteConfig = require("../server/config");

module.exports = {
    remote: {
        type: "remote",
        url: "http://localhost" + ":" + remoteConfig.port,
        settings: {
            apis: {
            },
            resolveResponse: function(res) {
                if(res.ok) {
                    return res.json()
                        .then(
                            (json) => {
                                if(json.error) {
                                    return Promise.reject(new Error(json.error.message));
                                }
                                else {
                                    return Promise.resolve(json);
                                }
                            }
                        )
                }
                else {
                    return Promise.reject(new Error("服务器连接超时"));
                }
            }
        }
    },
    mysql : {
        type: "mysql",
        url: "mysql://root@localhost:3306/testorm",
        settings: {
            "connection.pool": true
        }
    },
}
