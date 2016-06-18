"use strict";

const schema = {
    info: {
        source: "mysql",
        attributes: {
            infomation:{
                type: "object"
            }
        }
    },
    infoInMongodb: {
        source: "mongodb",
        attributes: {
            infomation:{
                type: "object"
            }
        }
    }
};


module.exports = schema;
