/**
 * Created by Administrator on 2016/7/4.
 */
"use strict";
const express = require("express");
const router = express.Router();
const constants = require("../constants");
const apis = constants.defaultRemoteApis;

class Common {
    constructor() {
        this._router = router;
    }

    init(uda, originSchema) {
        this._router.post(apis.urlFind, (req, res, next) => {
            let data = req.body;
            uda.findByExecTreeDirectly(data.name, data.execTree, data.indexFrom, data.count)
                .then(
                    (result) => {
                        res.json(result);
                    },
                    (err) => {
                        next(err);
                    }
                );
        });


        this._router.post(apis.urlInsert, (req, res, next) => {
            let data = req.body;
            uda.insert(data.name, data.data)
                .then(
                    (result) => {
                        res.json(result);
                    },
                    (err) => {
                        next(err);
                    }
                );
        });

        this._router.post(apis.urlUpdate, (req, res, next) => {
            let data = req.body;
            uda.update(data.name, data.updatePart, data.query)
                .then(
                    (result) => {
                        res.json(result);
                    },
                    (err) => {
                        next(err);
                    }
                );
        });

        this._router.post(apis.urlUpdateOneById, (req, res, next) => {
            let data = req.body;
            uda.updateOneById(data.name, data.updatePart, data.id)
                .then(
                    (result) => {
                        res.json(result);
                    },
                    (err) => {
                        next(err);
                    }
                );
        });


        this._router.post(apis.urlRemove, (req, res, next) => {
            let data = req.body;
            uda.remove(data.name, data.query)
                .then(
                    (result) => {
                        res.json(result);
                    },
                    (err) => {
                        next(err);
                    }
                );
        });


        this._router.post(apis.urlRemoveOneById, (req, res, next) => {
            let data = req.body;
            uda.removeOneById(data.name, data.id)
                .then(
                    (result) => {
                        res.json(result);
                    },
                    (err) => {
                        next(err);
                    }
                );
        });


        this._router.post(apis.urlKeyName, (req, res, next) => {
            let data = req.body;
            try{
                res.json(uda.getKeyName(data.name));
            }
            catch (err) {
                next(err);
            }
        });

        this._router.post(apis.urlKeyType, (req, res, next) => {
            let data = req.body;
            try{
                res.json(uda.getKeyType(data.name));
            }
            catch (err) {
                next(err);
            }
        });

        this._router.get(apis.urlSchemas, (req, res, next) => {

            res.json(originSchema);
        });

    }

    get router() {
        return this._router;
    }
}

const common = new Common();

module.exports = common;
