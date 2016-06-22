/**
 * Created by Administrator on 2016/6/22.
 */
"use strict";

const UDA = require("../../../src/UnifiedDataAccess");
const uda = new UDA();
const ds = require("../dataSources/dsLocal");
const schema = require("../schemas/schemaLocal");

const pick = require("lodash/pick");

function initialize() {
    return uda.connect(ds)
        .then(
            () => {
                uda.setSchemas(schema);
                return uda.dropSchemas()
                    .then(
                        () => {
                            return uda.createSchemas();
                        },
                        (err) => {
                            return Promise.reject(err);
                        }
                    )
            },
            (err) => {
                return Promise.reject(err);
            }
        );
}

function destroy() {
    return uda.disconnect();
}

function insert(name, data) {
    return uda.insert(name, data);
}

function update(name, updatePart, query) {
    return uda.update(name, updatePart, query);
}

function updateOneById(name, updatePart, id) {
    return uda.updateOneById(name, updatePart, id);
}

function remove(name, query) {
    return uda.remove(name, query);
}

function removeOneById(name, id) {
    return uda.removeOneById(name, id);
}

function find(name, projection, query, sort, indexFrom, count) {
    return uda.find(name, projection, query, sort, indexFrom, count);
}

function findOneById(name, projection, id) {
    return uda.findOneById(name, projection, id);
}

function findByExecTreeDirectly(name, execTree, indexFrom, count) {
    return uda.findByExecTreeDirectly(name, execTree, indexFrom, count);
}

function getSchemas(names) {
    return pick(schema, names);
}

function getKeyName(tblName) {
    return uda.getKeyName(tblName);
}

function getKeyType(tblName) {
    return uda.getKeyType((tblName));
}

module.exports = {
    initialize,
    destroy,
    insert,
    update,
    updateOneById,
    remove,
    removeOneById,
    find,
    findOneById,
    findByExecTreeDirectly,
    getSchemas,
    getKeyName,
    getKeyType
}