/**
 * Created by Administrator on 2016/6/7.
 */
"use strict";

class JoinDestructor{
    constructor() {

    }

    destruct(name, projection, query, schemas) {
        let result = {
            name
        };
        for(let attr in projection) {
            if(typeof projection[attr] === "object") {

            }
        }
    }
};




let joinDestructor = new JoinDestructor();


module.exports = joinDestructor;
