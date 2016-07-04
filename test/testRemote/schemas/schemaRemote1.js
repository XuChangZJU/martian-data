/**
 * Created by Administrator on 2016/6/21.
 */
"use strict";

module.exports = {
    user: {
        source: "remote",
        attributes: {
            name: {
                type: "string",
                required: true
            },
            age: {
                type: "int"
            }
        }
    },
    account: {
        source: "remote",
        attributes: {
            owner: {
                type: "ref",
                ref: "user",
                required: true
            },
            deposit: {
                type: "number",
                required: true
            }
        }
    }
};
