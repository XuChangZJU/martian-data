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

    init(option) {
        const {uda, schema} = option;
        this._router.post(apis.urlFind, (req, res, next) => {
            let data = req.body;
            try {
                uda.findByExecTreeDirectly(data.name, data.execTree, data.indexFrom, data.count, data.isCounting)
                    .then(
                        (result) => {
                            res.json(result);
                        }
                    )
                    .catch(
                        (err) => {
                            next(err);
                        }
                    );
            }
            catch (err) {
                next(err);
            }
        });


        this._router.post(apis.urlInsert, (req, res, next) => {
            let data = req.body;
            try {
                uda.insert({
                        name: data.name,
                        data: data.data
                    })
                    .then(
                        (result) => {
                            res.json(result);
                        }
                    )
                    .catch(
                        (err) => {
                            next(err);
                        }
                    );
            }
            catch (err) {
                next(err);
            }
        });

        this._router.post(apis.urlUpdate, (req, res, next) => {
            let data = req.body;
            try {
                uda.update({
                        name: data.name, data: data.updatePart, query: data.query
                    })
                    .then(
                        (result) => {
                            res.json(result);
                        }
                    )
                    .catch(
                        (err) => {
                            next(err);
                        }
                    );
            }
            catch (err) {
                next(err);
            }
        });

        this._router.post(apis.urlUpdateOneById, (req, res, next) => {
            let data = req.body;
            try {
                uda.updateOneById({
                        name: data.name, data: data.updatePart, id: data.id
                    })
                    .then(
                        (result) => {
                            res.json(result);
                        }
                    )
                    .catch(
                        (err) => {
                            next(err);
                        }
                    );
            }
            catch (err) {
                next(err);
            }
        });


        this._router.post(apis.urlRemove, (req, res, next) => {
            let data = req.body;
            try {
                uda.remove({
                        name: data.name, query: data.query
                    })
                    .then(
                        (result) => {
                            res.json(result);
                        }
                    )
                    .catch(
                        (err) => {
                            next(err);
                        }
                    );
            }
            catch (err) {
                next(err);
            }
        });


        this._router.post(apis.urlRemoveOneById, (req, res, next) => {
            let data = req.body;
            try {
                uda.removeOneById({
                        name: data.name, id: data.id
                    })
                    .then(
                        (result) => {
                            res.json(result);
                        }
                    )
                    .catch(
                        (err) => {
                            next(err);
                        }
                    );
            }
            catch (err) {
                next(err);
            }
        });


        this._router.post(apis.urlKeyName, (req, res, next) => {
            let data = req.body;
            try {
                res.json(uda.getKeyName(data.name));
            }
            catch (err) {
                next(err);
            }
        });

        this._router.post(apis.urlKeyType, (req, res, next) => {
            let data = req.body;
            try {
                res.json(uda.getKeyType(data.name));
            }
            catch (err) {
                next(err);
            }
        });

        this._router.post(apis.urlSchemas, (req, res, next) => {
            res.json(schema);
        });

        // this._router.post(apis.urlDeleteStorage, (req, res, next)=> {
        //     try {
        //         uda.disableStorageBy(req.body.entity, req.body.query);
        //     }
        //     catch (err) {
        //         next(err);
        //     }
        //     res.json({success: true});
        // })

    }

    get router() {
        return this._router;
    }

    get urls() {
        return constants.defaultRemoteUrls;
    }
}

const common = new Common();

module.exports = common;
