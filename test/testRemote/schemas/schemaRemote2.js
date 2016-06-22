/**
 * Created by Administrator on 2016/6/21.
 */
"use strict";

module.exports = {
    user: {
        source: "remote"
    },
    account: {
        source: "remote"
    },
    order: {
        source: "mysql",
        attributes: {
            account: {
                type: "ref",
                ref: "account"
            },
            time: {
                type: "date",
                required: true
            }
        }
    }
};
