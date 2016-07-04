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

module.exports = {
    initialize,
    destroy,
    uda,
    schema
}