/**
 * Created by Administrator on 2016/6/2.
 */
"use strict";

const schema = {
    house: {
        source: "mongodb",
        attributes: {
            buildAt:{
                type: "date",
                required : true,
                defaultValue : new Date()
            },
            status: {
                type: {
                    type: "enum",
                    values: ["verifying", "offline", "free", "inRent"],
                },
                required: true,
                defaultValue: "verifying"
            }
        }
    }
};


module.exports = schema;
