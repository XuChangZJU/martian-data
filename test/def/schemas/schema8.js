"use strict";

const schema = {
    info: {
        source: "mysql",
        attributes: {
            information:{
                type: "object"
            },
            name: {
                type: "text"
            }
        }
    }
};


module.exports = schema;
