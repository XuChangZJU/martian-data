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
                key: true,
                defaultValue : new Date()
            },
            status: {
                type: {
                    type: "enum",
                    values: ["verifying", "offline", "free", "inRent"]
                },
                required: true,
                defaultValue: "verifying"
            }
        }
    },
    houseInfo: {
        source: "mysql",
        attributes: {
            area:{
                type: "number",
                required : true
            },
            floor: {
                type: "number",
                required: true
            }
        }
    }
};


module.exports = schema;
