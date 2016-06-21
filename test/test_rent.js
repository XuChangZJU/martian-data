/**
 * Created by Administrator on 2016/6/19.
 */
"use strict";

var expect = require("expect.js");

const uda = require("../src/UnifiedDataAccess");
const dataSource = require("./def/dataSource");
const schema = require("./def/schemas/schema7");

const g_houseInfos = [
    // 房屋基本信息
    {
        "prop":{
            "title": {
                val: "西溪雅苑"
            },
            "acreage" :{
                val: 120
            }
        },
        "demand": {              // 出租需求
            "price" : 2000,
            "onHire": Date.now()
        }
    },
    {
        "prop":{
            "title": {
                val: "西溪雅苑"
            },
            "acreage" :{
                val: 120
            }
        },
        "demand": {              // 出租需求
            "price" : 2000,
            "onHire": Date.now()
        }
    }
];

const g_houses = [
    {
        status: "unfinished"
    },
    {
        status: "notThrough"
    }
];

const g_keys = [
    {
        name: "wo de yao shi"
    },
    {
        name: "no zuo no deal"
    }
];

const g_users = [
    {
        name: "荆轲",
        age: null
    },
    {
        name: "郭靖",
        age: null
    },
    {
        name: "李世民",
        age: 34
    },
    {
        name: "刘备",
        age: 29
    }
];
const g_consults = [
    {
        details :[]
    },
    {
        details :[]
    }
];

function initData(uda, users, keys, houses, houseInfos) {
    return uda.dropSchemas()
        .then(
            () => {
                return uda.createSchemas()
                    .then(
                        () => {
                            let promises = [];
                            users.forEach(
                                (ele, idx) => {
                                    const idx2 = idx;
                                    promises.push(
                                        uda.insert("user", ele)
                                            .then(
                                                (result) => {
                                                    users[idx2] = result;
                                                    return Promise.resolve();
                                                },
                                                (err) => {
                                                    return Promise.reject(err);
                                                }
                                            )
                                    );
                                }
                            );
                            return Promise.all(promises)
                                .then(
                                    ()=>{
                                        let promises2 = [];
                                            keys.forEach(
                                                (key, index) => {
                                                    let user = users[index];
                                                    keys[index].ownerId=user.id||user._id;
                                                    promises2.push(
                                                        uda.insert("key", keys[index])
                                                            .then(
                                                                (key) => {
                                                                    keys[index] = key;
                                                                    return Promise.resolve();
                                                                },
                                                                (err) => {
                                                                    return Promise.reject(err);
                                                                }
                                                            )
                                                    )
                                                }
                                            );
                                        return Promise.all(promises2).then(
                                            ()=>{
                                                let promises3 = [];
                                                houseInfos.forEach(
                                                    (houseInfo, index) => {
                                                        let key = keys[index];
                                                        houseInfo.status =houses[index].status;
                                                        promises3.push(
                                                            uda.insert("houseInfo", houseInfo)
                                                                .then(
                                                                    (houseInfo) => {
                                                                        houseInfos[index]=houseInfo;
                                                                        houses[index].houseInfoId = houseInfo.id || houseInfo._id;
                                                                        houses[index].ownerId = key.ownerId;
                                                                        houses[index].masterKeyId =  key.id || key._id;
                                                                       
                                                                        return uda.insert("house", houses[index])
                                                                            .then(
                                                                                (house)=>{
                                                                                    houses[index]=house;
                                                                                    return Promise.resolve();
                                                                                },
                                                                                (err)=>{
                                                                                    return Promise.reject(err);
                                                                                }
                                                                            )

                                                                    },
                                                                    (err) => {
                                                                        return Promise.reject(err);
                                                                    }
                                                                )
                                                        )
                                                    }
                                                );
                                                return Promise.all(promises3);
                                            },
                                            (err)=>{
                                                return Promise.reject(err);
                                            }
                                        )
                                    },
                                    (err)=>{
                                        return Promise.reject(err);
                                    }
                                )
                        },
                        (err) => {
                            return Promise.reject(err);
                        }
                    )
            },
            (err) => {
                return Promise.reject(err);
            }
        )
}


describe("test rent", () => {
    describe("rent create schema with refs", function() {
        this.timeout(4000);
        before((done) => {
            uda.connect(dataSource)
                .then(done);
        });

        it("[1.0]rent create house in mysql", (done) => {

            let _schema = JSON.parse(JSON.stringify(schema));
            uda.setSchemas(_schema);
            uda.dropSchemas()
                .then(
                    () => {
                        uda.createSchemas()
                            .then(
                                () => {
                                    console.log("请查看表");
                                    done();
                                },
                                (err) => {
                                    done(err);
                                }
                            );

                    },
                    (err) => {
                        done(err);
                    }
                );
        });
    });

    describe("rent select ", function() {
        this.timeout(5000);
        const houses =  JSON.parse(JSON.stringify(g_houses));
        const users = JSON.parse(JSON.stringify(g_users));
        const houseInfos = JSON.parse(JSON.stringify(g_houseInfos));
        const keys = JSON.parse(JSON.stringify(g_keys));
        before((done) => {
            uda.connect(dataSource)
                .then(
                    (result) => {

                        let _schema = JSON.parse(JSON.stringify(schema));
                        uda.setSchemas(_schema);
                        initData(uda, users, keys, houses, houseInfos)
                            .then(
                                () => {
                                    done();
                                },
                                (err) => {
                                    done(err);
                                }
                            );
                    },
                    (err) => {
                        done(err);
                    }
                );
        });

        it("[1.0]rent select house in mysql", (done) => {
            //    console.log(keys);
            //   console.log(houseInfos);
            console.log(houses);
            const query = {
                owner: {
                    name: "荆轲"
                }
            };
            const projection = {
                id: 1,
                status: 1,
                owner:{
                    name:1
                },
                key:{
                    name:1
                }
            };
            const sort = {
                houseInfo: {
                    _createAt_: 1
                }
            };
            const indexFrom = 0, count = 2;

            uda.find("house", projection, query, sort, indexFrom, count)
                .then(
                    (result) => {
                        console.log(result);
                        done();
                    },
                    (err) => {
                        done(err);
                    }
                )
        });

        it("[1.1]rent select house by ownerId in mysql", (done) => {

            const query = {
                owner: {
                    _id: houses[0].ownerId
                }
            };
            const projection = {
                id: 1,
                status: 1,
                owner:{
                    name:1
                },
                key:{
                    name:1
                }
            };
            const sort = {
                key: {
                    _createAt_: -1
                }
            };
            const indexFrom = 0, count = 2;

            uda.find("house", projection, query, sort, indexFrom, count)
                .then(
                    (result) => {
                        console.log(result);
                        done();
                    },
                    (err) => {
                        done(err);
                    }
                )
        });

        it("[1.2]rent select house by id in mysql", (done) => {
       
            const projection = {
                id: 1,
                status: 1,
                ownerId: 1,
                masterKeyId: 1,
                houseInfoId: 1
            };

            const projection1 = {
                _id: 1,
                prop: 1 
            };

            const query2 = {
             
            };
            const projection2 = {
                id: 1,
                status: 1,
                owner:{
                    name:1
                },
                key:{
                    name:1
                }
            };
            const sort2 = {
                houseInfo: {
                    _createAt_: -1
                }
            };
            const indexFrom2 = 0, count2 = 10;
            
            uda.findById("house", projection, houses[1].id)
                .then(
                    (house) => {
                        console.log(house);
                  
                        uda.findById(
                            "houseInfo",
                            projection1,
                            house.houseInfoId
                        ).then(
                            (houseInfo) => {
                                console.log(houseInfo);
                                uda.updateOneById(
                                    "houseInfo",
                                    {
                                        $set: {
                                            prop: {
                                                title: {
                                                    val: "万家花城"
                                                },
                                                area: {
                                                    val: "杭州市"
                                                },
                                                addr: {
                                                    val: "萍水西街180号"
                                                }
                                            }
                                        }
                                    },
                                    houseInfo._id
                                ).then(
                                    (houseInfo) => {
                                        console.log(houseInfo);
                                        uda.removeOneById(
                                            "house",
                                            house.id
                                        ).then(
                                            (removeHouse)=>{
                                                console.log(removeHouse);
                                                uda.removeOneById(
                                                    "houseInfo",
                                                    house.houseInfoId
                                                ).then(
                                                    (result)=>{
                                                        console.log(result);
                                                        uda.find("house", projection2, query2, sort2, indexFrom2, count2)
                                                            .then(
                                                                (result) => {
                                                                    console.log(result);
                                                                    done();
                                                                },
                                                                (err) => {
                                                                    done(err);
                                                                }
                                                            )
                                                    },
                                                    (err)=>{
                                                        done(err);
                                                    }
                                                )
                                            },
                                            (err)=>{
                                                done(err); 
                                            }
                                        )
                                    },
                                    (err) => {
                                        done(err);
                                    }
                                )
                            },
                            (err) => {
                                done(err);
                            }
                        )

                    },
                    (err) => {
                        done(err);
                    }
                )
        });

        it("[1.3]rent update house by id in mysql", (done) => {

            const projection = {
                id: 1,
                status: 1,
                ownerId: 1,
                masterKeyId: 1,
                houseInfoId: 1
            };

            
            uda.findById("house", projection, houses[0].id)
                .then(
                    (house) => {
                        console.log(house);
                        
                        uda.updateOneById(
                            "house",
                            {
                                $set: {
                                    status: "verifying"
                                }
                            },
                            house.id
                        ).then(
                            (house1)=>{
                                console.log(house1);
                                uda.updateOneById(
                                    "houseInfo",
                                    {
                                        $set:{
                                            status:house1.status
                                        }
                                    },
                                    house.houseInfoId
                                ).then(
                                    (houseInfo)=>{
                                        console.log(houseInfo);
                                        done();
                                    },
                                    (err)=>{
                                        done(err);
                                    }
                                )
                            },
                            (err)=>{
                                done(err);  
                            }
                        )

                    },
                    (err) => {
                        done(err);
                    }
                )
        });

        it("[1.4]rent insert consult in mongodb", (done) => {
            g_consults[0].houseId=houses[0].id;
            g_consults[0].tenantId=users[2]._id||users[2].id;
            g_consults[0].details=[
                {
                    ownerId:users[2]._id||users[2].id,
                    content:"房东,房租还能便宜些吗?",
                    createAt:Date.now()
                }
            ];

            const landlord_answer={
                ownerId:houses[0].ownerId,
                content:"已经很便宜了，不能再便宜了，要租快租！",
                createAt:Date.now()
            };
            
            const query2 = {
               house :{}
            };
            const projection2 = {
                _id: 1,
                tenant:{
                    name:1
                },
                details:1
          
            };
            const sort2 = {
                tenant: {
                    _createAt_: -1
                }
            };
            const indexFrom2 = 0, count2 = 10;
            
            uda.insert("consult",g_consults[0]).then(
                 (consult)=>{
                     console.log(consult);
                     uda.updateOneById(
                         "consult",
                         {
                             $push:{
                                 'details':landlord_answer
                             }
                         },
                         consult._id
                     ).then(
                         (consult)=>{
                             console.log(consult);
                             query2.house.id =  consult.houseId;
                             console.log(query2);
                             uda.find("consult", projection2, query2, sort2, indexFrom2, count2)
                                 .then(
                                     (result) => {
                                         console.log(result);
                                         if(result&&result.length>0) {
                                             console.log(result.length);

                                             let promises = [];
                                             if (result[0].details && result[0].details.length > 0) {
                                                 result[0].details.forEach((ele1, index1)=> {
                                                     console.log(ele1);
                                                     promises.push(
                                                         uda.findById(
                                                             "user",
                                                             {
                                                                 _id: 1,
                                                                 name: 1
                                                             },
                                                             ele1.ownerId
                                                         ).then(
                                                             (user)=> {
                                                                 ele1.owner=user;
                                                                 delete ele1.ownerId;
                                                                 return Promise.resolve(user);
                                                             },
                                                             (err) => {
                                                                 return Promise.reject(err);
                                                             }
                                                         )
                                                     )
                                                 })
                                             }
                                             return Promise.all(promises).then(
                                                 (users)=> {
                                                     console.log(users);
                                                     console.log(result);
                                                     done();
                                                 },
                                                 (err) => {
                                                     done(err);
                                                 }
                                             )
                                         }

                                     },
                                     (err) => {
                                         done(err);
                                     }
                                 )
                         },
                         (err)=>{
                             done(err)
                         }
                     )
                 },
                 (err)=>{
                     done(err)
                 }
             )
        })
            
        
    });

});
