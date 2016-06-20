"use strict";

const schema = {
    info: {
        source: "mysql",
        attributes: {
            information:{
                type: "object"
            }
        }
    },
    infoInMongodb: {
        source: "mongodb",
        attributes: {
            information:{
                type: "object"
            }
        }
    }
};


module.exports = schema;
