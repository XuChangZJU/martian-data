/**
 * Created by Administrator on 2016/6/22.
 */
"use strict";
const express = require('express');
const router = express.Router();

const db = require("./db");
const schema = require("../schemas/schemaLocal");



router.post("/find", (req, res, next) => {
    let data = req.body;
    db.findByExecTreeDirectly(data.name, data.execTree, data.indexFrom, data.count)
        .then(
            (result) => {
                res.json(result);
            },
            (err) => {
                next(err);
            }
        );
});


router.post("/insert", (req, res, next) => {
    let data = req.body;
    db.insert(data.name, data.data)
        .then(
            (result) => {
                res.json(result);
            },
            (err) => {
                next(err);
            }
        );
});

router.post("/update", (req, res, next) => {
    let data = req.body;
    db.update(data.name, data.updatePart, data.query)
        .then(
            (result) => {
                res.json(result);
            },
            (err) => {
                next(err);
            }
        );
});

router.post("/updateOneById", (req, res, next) => {
    let data = req.body;
    db.updateOneById(data.name, data.updatePart, data.id)
        .then(
            (result) => {
                res.json(result);
            },
            (err) => {
                next(err);
            }
        );
});


router.post("/remove", (req, res, next) => {
    let data = req.body;
    db.remove(data.name, data.query)
        .then(
            (result) => {
                res.json(result);
            },
            (err) => {
                next(err);
            }
        );
});


router.post("/removeOneById", (req, res, next) => {
    let data = req.body;
    db.removeOneById(data.name, data.id)
        .then(
            (result) => {
                res.json(result);
            },
            (err) => {
                next(err);
            }
        );
});

router.post("/removeOneById", (req, res, next) => {
    let data = req.body;
    db.removeOneById(data.name, data.id)
        .then(
            (result) => {
                res.json(result);
            },
            (err) => {
                next(err);
            }
        );
});

router.post("/keyName", (req, res, next) => {
    let data = req.body;
    try{
        res.json(db.getKeyName(data.name));
    }
    catch (err) {
        next(err);
    }
});

router.post("/keyType", (req, res, next) => {
    let data = req.body;
    try{
        res.json(db.getKeyType(data.name));
    }
    catch (err) {
        next(err);
    }
});

router.post("/schemas", (req, res, next) => {
    let data = req.body;
    
    res.json(db.getSchemas(data));
});

module.exports = router;