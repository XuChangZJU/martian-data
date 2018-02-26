/**
 * Created by Administrator on 2016/6/4.
 */
"use strict";

const apis = {
    urlFind: "/find",
    urlInsert: "/insert",
    urlUpdate: "/update",
    urlUpdateOneById: "/updateOneById",
    urlRemove: "/remove",
    urlRemoveOneById: "/removeOneById",
    urlKeyName: "/keyName",
    urlKeyType: "/keyType",
    urlSchemas: "/schemas",
    urlDeleteStorage: "/deleteStorage"
};

let urls = [];

for (let i in apis) {
    urls.push({
        url: apis[i],
        methods: {
            "POST": 1
        }
    });
}

module.exports = {
    deleteAtColumn: "_deleteAt_",
    createAtColumn: "_createAt_",
    updateAtColumn: "_updateAt_",
    mysqlDefaultIdColumn: "id",
    mongodbDefaultIdColumn: "_id",
    typeReference: "ref",
    defaultRemoteApis: apis,
    defaultRemoteApiRouter: '/client',
    defaultRemoteUrls: urls,
    parellelCount: 50,
    parellelIndex: 0,
    queue: [],
    storageInterval: 5 * 6000
}
;
