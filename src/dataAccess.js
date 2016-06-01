/**
 * Created by Administrator on 2016/6/1.
 */
"use strict";
var orm = require('orm');

class DataAccess {
    setDataSource(_ds) {
        this.ds = _ds;

        // 连接上所有的数据库
        let promises = [];
        for(let i in this.ds) {
            const dsItem = this.ds[i];
            const name = i;
            promises.push(new Promise(
                (resolve, reject) => {
                    orm.connect(dsItem.url, (err, db) => {
                        if(err) {
                            reject(err);
                        }
                        else {
                            if(dsItem.settings) {
                                for(let j in dsItem.settings) {
                                    db.settings.set(j, dsItem.settings[j]);
                                }
                            }
                            resolve({
                                name,
                                db
                            });
                        }
                    });
                }
            ));
        }
        return Promise.all(promises)
            .then(
                (connections) => {
                    this.connections = {};
                    connections.forEach((ele, index) => {
                        this.connections[ele.name] = ele.db;
                    })
                    return Promise.resolve();
                }
            );
    }

    setSchemaDefinition(_schemas) {
        this.schema = _schemas;
    }
};


const dataAccess = new DataAccess();






module.exports = dataAccess;