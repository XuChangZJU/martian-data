/**
 * Created by Administrator on 2016/6/22.
 */
"use strict";
let express = require('express');
let process = require("process");
let bodyParser = require('body-parser');
let app = express();
const db = require("./db");
const config = require("./config");

const port = config.port;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));


function initialize() {
    return new Promise(
        (resolve, reject) => {
            db.initialize()
                .then(
                    () => {
                        // 使用Common router;
                        let commonRouter = require("../../../src/routers/common");
                        commonRouter.init(db.uda, db.schema);
                        app.use(config.url, commonRouter.router);


                        app.get('/', function (req, res) {
                            res.send('Hello World!');
                        });


                        app.use(function(err, req, res, next){
                            // whatever you want here, feel free to populate
                            // properties on `err` to treat it differently in here.

                            //todo 以后要换成其他logger模块
                            console.error(err);
                            res.status(err.status || 200);

                            res.send({
                                error: {
                                    message: err.message,
                                    code: err.code
                                }
                            });



                        });

// our custom JSON 404 middleware. Since it's placed last
// it will be the last middleware called, if all others
// invoke next() and do not respond.
                        app.use(function(req, res){
                            res.status(200);
                            res.send({
                                error: {
                                    code: 9999,
                                    message: "调用了不存在的API"
                                }
                            });
                        });

                        var server = app.listen(port, function () {
                            var host = server.address().address;
                            var port = server.address().port;

                            console.log('Example app listening at http://%s:%s', host, port);
                            resolve();
                        });

                        process.on("SIGTERM", () => {
                            process.exit();
                        })
                    },
                    (err) => {
                        reject(err);
                    }
                );
        }
    )
}

initialize()
    .then(
        () => {
            process.send({success: true});
        },
        (error) => {
            process.send({success: false, error: error.message});
        }
    );

