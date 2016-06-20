/**
 * Created by Administrator on 2016/6/8.
 */
"use strict";



function resolveAttribute(result, attr, value) {
    const i = attr.indexOf(".");
    if(i !== -1) {
        const attrHead = attr.slice(0, i);
        const attrTail = attr.slice(i + 1);
        if(!result[attrHead]) {
            result[attrHead] = {};
        }

        resolveAttribute(result[attrHead], attrTail, value);
    }
    else {
        result[attr] = value;
    }
}



function transformResultObject(result) {
    let result2 = {};
    for(let attr in result) {
        const value = result[attr];
        resolveAttribute(result2, attr, value);
    }

    return result2;
}

class ResultTransformer{
    transformSelectResult(result) {
        if(result instanceof Array) {
            return result.map(
                (ele, index) => {
                    return transformResultObject(ele);
                }
            )
        }
        else {
            return transformResultObject(result);
        }
    }
};


let resultTransformer = new ResultTransformer();

module.exports = resultTransformer;
