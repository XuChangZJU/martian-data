/**
 * SystemWarden 系统守望者
 * 本模块用于维护系统数据间的一致性
 * coded by Xc  20161209
 *
 */
'use strict';
const assert = require('assert');
const assign = require('lodash/assign');
const keys = require('lodash/keys');
const groupBy = require('lodash/groupBy');
const pick = require('lodash/pick');
const omit = require('lodash/omit');
const values = require('lodash/values');
const flatten = require('lodash/flatten');
const unset = require('lodash/unset');
const e = require('express');
const ObjectID = require("mongodb").ObjectID;
require("./utils/promiseUtils");
const ITER_COUNT = 64;

function emptyDtLocalAttr(trigger, entity, remoteEntity, txn) {
    return this.uda.getSource(this.txnSource).execSql(
        `select * from ${trigger.entity} where id = ${entity.id} for update`, true, txn.txn)
        .then(
            (entityInfo)=> {
                assert(entityInfo.length === 1);
                entityInfo[0][trigger.dtLocalAttr] = JSON.parse(entityInfo[0][trigger.dtLocalAttr]);
                if (!(entityInfo[0][trigger.dtLocalAttr] instanceof Array)) {
                    console.log(entityInfo);
                }
                assert(entityInfo[0][trigger.dtLocalAttr] instanceof Array);
                const destDtr = entityInfo[0][trigger.dtLocalAttr].find((ele)=>ele.endsWith(trigger.id));
                const dtrIndex = entityInfo[0][trigger.dtLocalAttr].indexOf(destDtr);
                let data = {
                    //  执行完一个txn，清除自己的txn标记
                    [trigger.dtLocalAttr]: dtrIndex === -1 ? entityInfo[0][trigger.dtLocalAttr] : entityInfo[0][trigger.dtLocalAttr].slice(0, dtrIndex).concat(entityInfo[0][trigger.dtLocalAttr].slice(dtrIndex + 1, entityInfo[0][trigger.dtLocalAttr].length))
                };
                if (data[[trigger.dtLocalAttr]].length === 0) {
                    data = assign({}, data, {txnState: 10});
                }
                if (remoteEntity && trigger.hasOwnProperty('localJoinCol')) {
                    if (trigger.localAppend) {
                        if (!(remoteEntity instanceof Array)) {
                            assert(remoteEntity instanceof Object);
                            remoteEntity = [remoteEntity];
                        }
                        if (!trigger.localJoinCol) {
                            throw new Error("存在localAppend属性，必须传入【" + trigger.localJoinCol + "】");
                        }
                        data[trigger.localJoinCol] = entity[trigger.localJoinCol].concat(
                            remoteEntity.map((ele) => ele.id));
                    }
                    else {
                        data[trigger.localJoinCol] = remoteEntity.id;
                    }
                }
                return this.updateEntity(trigger.entity, data, entityInfo[0].id, txn);
            }
        )
}

function PromisesWithSerial(promises) {
    const RESULT = [];
    let errIdx = -1;
    if (!promises || promises.length === 0) {
        return Promise.resolve([]);
    }

    const promisesIter = (idx) => {
        if (idx === promises.length) {
            return Promise.resolve();
        }
        return promises[idx].fn.apply(promises[idx].me, promises[idx].params)
            .then(
                (result) => {
                    RESULT.push(result);
                    if (result > 0) {
                        console.log(`触发器【${promises[idx].params[0].name}】结束，共有【${result}】条数据在同一事务内被处理`);
                    }
                    return promisesIter(idx + 1);
                }
            ).catch(
                (err) => {
                    RESULT.push(err);
                    errIdx = idx;
                    console.error(`触发器【${promises[idx].params[0].name}】发生异常，错误是${err.stack}`);
                    return promisesIter(idx + 1);
                }
            );
    };

    return promisesIter(0)
        .then(
            () => {
                if (errIdx === -1) {
                    return RESULT;
                }
                throw RESULT[errIdx];
            }
        );
}

function doTrigger(trigger, entity, txn, preEntity, context) {
    //  因为现在一条数据由txnUuid一个任务队列完成，需要定位到触发器对应的txn
    let dtr = entity[trigger.dtLocalAttr] && entity[trigger.dtLocalAttr].find((ele)=>ele.endsWith(trigger.id));
    if (trigger.create) {
        if (trigger.volatile && trigger.dtLocalAttr && trigger.remoteEntity && trigger.dtRemoteAttr && dtr) {
            return this.uda.find({
                name: trigger.remoteEntity,
                projection: {
                    id: 1,
                },
                query: {
                    [trigger.dtRemoteAttr]: dtr,
                }, indexFrom: 0, count: 100
            }).then(
                (result) => {
                    if (!result || result.length === 0) {
                        return trigger.create(entity, txn, preEntity, context);
                    }
                    //  todo    这里存在问题，local的一个txnId对应于远端的一个operation。而不是一个entity。这里可能会有多个。暂且先返回第一个，代码中规避
                    // assert(result.length === 1);
                    return Promise.resolve(result[0]);
                }
            );
        }
        return trigger.create(entity, txn, preEntity, context);
    }
    else if (trigger.update || trigger.remove) {
        const fn = trigger.update || trigger.remove;
        const doTriggerInner = (indexFrom) => {
            return this.uda.find({
                    name: trigger.triggerEntity,
                    projection: trigger.triggerProjection && trigger.triggerProjection(entity),
                    query: trigger.triggerWhere(entity),
                    indexFrom,
                    count: ITER_COUNT,
                    forUpdate: true,
                    txn
                }
            ).then(
                (list) => {
                    let promise;
                    if (list.length === 0 && !trigger.haveToCall) {
                        promise = Promise.resolve(0);
                    }
                    else if (trigger.hasOwnProperty('inGroup') && trigger.inGroup === true) {
                        promise = fn(entity, list, txn, preEntity, context);
                    }
                    else {
                        /**
                         * 同一个事务对同一个触发器的多行处理还是串行
                         * 以防止不必要的幻像读
                         * @param indexFrom2
                         */
                        let count = 0;
                        const doTriggerIter = (indexFrom2) => {
                            if (indexFrom2 === list.length) {
                                return Promise.resolve(count);
                            }
                            return fn(entity, list[indexFrom2], txn, preEntity, context)
                                .then(
                                    (count2) => {
                                        count += count2;
                                        return doTriggerIter(indexFrom2 + 1);
                                    }
                                );
                        };
                        promise = doTriggerIter(0);
                    }
                    return promise.then(
                        (count) => {
                            const count2 = typeof count === 'number' ? count : 0;
                            if (list.length === ITER_COUNT) {
                                return doTriggerInner(indexFrom + ITER_COUNT)
                                    .then(
                                        (count3) => Promise.resolve(count3 + count2)
                                    );
                            }
                            return Promise.resolve(count2);
                        }
                    );
                }
            );
        };
        if (trigger.volatile && trigger.dtLocalAttr && trigger.remoteEntity && trigger.dtRemoteAttr && dtr) {
            return this.uda.find({
                name: trigger.remoteEntity,
                projection: {
                    id: 1,
                },
                query: {
                    [trigger.dtRemoteAttr]: dtr,
                },
                indexFrom: 0, count: 100
            }).then(
                (result) => {
                    if (!result || result.length === 0) {
                        return doTriggerInner(0);
                    }
                    assert(result.length !== 0);
                    return Promise.resolve(result);
                }
            );
        }

        return doTriggerInner(0);
    }
    // else if (trigger.waitSomeOne && trigger.todoTogether) {
    //     const exsitedEle = this.execPool.get(trigger.waitSomeOne(entity, txn));
    //     if (!exsitedEle) {
    //         console.log(`运行池中创建了【1】个事件`);
    //         this.execPool.set(trigger.waitSomeOne(entity, txn), {
    //             injectedAt: Date.now(),
    //             func: trigger.todoTogether,
    //             params: [entity],
    //             entities: [entity],
    //             trigger: trigger,
    //         });
    //     }
    //     else {
    //         console.log(`运行池中补充了【1】个已存在的事件`);
    //         console.log(`当前运行池中有${this.execPool.size}个事件`);
    //         exsitedEle.entities = exsitedEle.entities.concat(entity);
    //     }
    // }
    return Promise.resolve();
}

function doVolatileLogic(trigger, entity, preEntity) {
    return this.uda.startTransaction("mysql")
        .then(
            (txn) => {
                return doTrigger.call(this, trigger, entity, txn, preEntity)
                    .then(
                        (result) => {
                            // 成功后将dtLocalAttr更新为null，若是在运行池的trigger，由运行池自身更新dtLocalAttr
                            // if (trigger.dtLocalAttr && !trigger.intoPool) {
                            if (trigger.dtLocalAttr) {
                                // dtLocaAttr未必一定要有，如果不需要保证最终一致性，则可以不定
                                return emptyDtLocalAttr.call(this, trigger, entity, trigger.create ? result : undefined, txn)
                                    .then(
                                        () => {
                                            if (trigger.localJoinCol) {
                                                return Promise.resolve(1);
                                            }
                                            return Promise.resolve(result);
                                        }
                                    )
                            }
                            return Promise.resolve(result);
                        }
                    )
                    .then(
                        (result) => this.uda.commitTransaction(txn)
                            .then(
                                () => result
                            )
                    )
                    .catch(
                        (err) => {
                            console.error(err);
                            return this.uda.rollbackTransaction(txn)
                                .then(
                                    () => Promise.reject(err)
                                );
                        }
                    );
            }
        );
}

function execTrigger(trigger, entity, txn, preEntity, context) {
    if (trigger.volatile) {
        const onTxnCommitted = (txn2) => {
            if (txn2 === txn) {
                this.uda.removeListener('txnCommitted', onTxnCommitted);

                return doVolatileLogic.call(this, trigger, entity, preEntity, context);
            }
        };

        this.uda.on('txnCommitted', onTxnCommitted);
        return Promise.resolve(0);
    }
    else {
        return doTrigger.call(this, trigger, entity, txn, preEntity, context)
    }
}

/**
 * 新的execWatcher，作了简化以及singltone的处理，去掉了超时判断（可能会有问题）
 * @param {*} watcher 
 * @param {*} entityId 
 */
async function execWatcher(watcher, entityId) {
    const { where, projection, entity, forceIndex, forUpdate, name,
        trigger, inGroup, maxCount, singleton, beginsAt } = watcher;
    const query = where();
    if (entityId !== null && entityId !== undefined) {
        assign(where, { id: entityId });
    }

    let myBeginsAt = Date.now();
    if (beginsAt) {
        console.warn(`watcher「${name}」发生了重叠性运行，请注意`);
        if (singleton) {
            console.warn(`watcher「${name}」只允许单实例执行，故中止`);
            return 0;
        }
    }
    else {
        assign(watcher, { beginsAt: myBeginsAt });
    }

    const txn = await this.uda.startTransaction(this.txnSource);
    let count;
    try {
        const rows = await this.uda.find({
            name: entity,
            projection,
            query,
            indexFrom : 0,
            count: maxCount | 1024,
            forceIndex: forceIndex,
            forUpdate,
            txn,
        });

        count = rows.length;
        if (rows.length > 0) {
            if (inGroup) {
                await trigger(rows, txn);
            }
            else {
                for (let row of rows) {
                    await trigger(row, txn);
                }
            } 
        }

        await this.uda.commitTransaction(txn);
    }
    catch(err) {
        await this.uda.rollbackTransaction(txn);
        throw err;
    }

    if (watcher.beginsAt === myBeginsAt) {
        unset(watcher, 'beginsAt');
    }
    console.log(`状态监听器【${name}】恢复了【${count}】条数据到一致状态，耗时${Date.now() - myBeginsAt}毫秒`);
    return count;
}

class SystemWarden {
    constructor(_uda, _txnSource) {
        this.uda = _uda;
        this.txnSource = _txnSource;
        this.itMap = new Map();     // insert triggers
        this.utMap = new Map();     // update triggers
        this.rtMap = new Map();     // remove triggers
        this.gtMap = new Map();     // get triggers
        // this.execPool = new Map();     // 运行池
        this.volatileArray = [];    // volatile triggers
        this.wtArray = [];          // watchers
        this.wtMap = new Map();     // watchers with ids
        this.volatileTriggerLatency = 60000;    // 默认volatile的latency为一分钟
        // this.execPoolLantency = 500;    //  注入运行池的时限为500
    }

    /**
     * 允许后注入uda
     * @param _uda
     * @param _txnSource
     */
    setUda(_uda, _txnSource) {
        this.uda = _uda;
        this.txnSource = _txnSource;
    }

    /**
     * 注册一个trigger
     * @param trigger
        {
               name: '当产生leasePay时，生成相应的order',
               action: 'insert'/'update'/'remove',
               entity: 'leasePay',
               attribute: 'state',             // update only
               valueCheck: () => {},
               create: (entity, txn) => {},
               update: (entity, triggerEntity, txn) => {},
               remove: (entity, triggerEntity, txn) => {},
               dtLocalAttr: 'orderTxnUuid',
               dtRemoteAttr: 'txnUuid',
               beforeAction: true,                      // 操作前触发
               volatile: true,                         // 这个域可以不填写，warden根据TxnSource来决定是否同源，但如果标识为non-volatile，则必须要同源！
               triggerEntity: 'order',
               triggerWhere: (entity) => {},            // update only
               triggerProjection: (entity) => {},       // update only
           }
     */
    registerTrigger(trigger) {
        assert(!trigger.volatile || !trigger.beforeAction); // 一个trigger不可能同时是volatile和beforeAction
        switch (trigger.action) {
            case 'insert':
            {
                let entityTriggers = this.itMap.get(trigger.entity);
                if (entityTriggers) {
                    entityTriggers.push(trigger);
                }
                else {
                    this.itMap.set(trigger.entity, [trigger]);
                }
                break;
            }
            case 'update':
            {
                assert(!trigger.valueCheck || typeof trigger.valueCheck === 'function');
                let entityMap = this.utMap.get(trigger.entity);
                if (entityMap) {
                    let attrTriggers = entityMap.get(trigger.attribute);
                    if (attrTriggers) {
                        attrTriggers.push(trigger);
                    }
                    else {
                        entityMap.set(trigger.attribute, [trigger]);
                    }
                }
                else {
                    entityMap = new Map();
                    const attrTriggers = [trigger];
                    entityMap.set(trigger.attribute, attrTriggers);
                    this.utMap.set(trigger.entity, entityMap);
                }
                break;
            }
            case 'remove':
            case 'delete':
            {
                let entityTriggers = this.rtMap.get(trigger.entity);
                if (entityTriggers) {
                    entityTriggers.push(trigger);
                }
                else {
                    this.rtMap.set(trigger.entity, [trigger]);
                }
                break;
            }
            case 'get': 
            {
                const entityTriggers = this.gtMap.get(trigger.entity);
                if (entityTriggers) {
                    entityTriggers.push(trigger);
                }
                else {
                    this.gtMap.set(trigger.entity, [trigger]);
                }
                break;
            }
            default:
                throw new Error('trigger的action属性必须是insert/update/delete之一');
        }
        assert(trigger.volatile || this.uda.schemas[trigger.entity].source === this.txnSource);
        assert(!(trigger.volatile && trigger.beforeAction), 'trigger的volatile和beforeAction不能同是为真');
        if (trigger.volatile || this.uda.schemas[trigger.entity].source !== this.txnSource) {
            assert(!trigger.hasOwnProperty("dtLocalAttr") || trigger.hasOwnProperty("id"));  // volatile且要求warden帮助实现最终一致性的Trigger必须有id，用于处理分布式一致性
            trigger.volatile = true;
            if (trigger.hasOwnProperty("dtLocalAttr")) {
                // 也只有需要保证最终一致性的trigger，需要被放入patrol查看的数组
                this.volatileArray.push(trigger);
            }
        }
    }

    /**
     * 注册一个Watcher
     * @param watcher
     * {
            name: '监听leasePay的相应order支付完成',
            entity: 'leasePay',
            where: () => {} ,
            projection: {},
            trigger: (entity, txn) => {}
        }
     */
    registerWatcher(watcher) {
        this.wtArray.push(watcher);
        if (watcher.hasOwnProperty("id")) {
            this.wtMap.set(watcher.id, watcher);
        }
    }

    /**
     * find带触发器
     * @param {*}} name 
     * @param {*} options 
     * @param {*} txn 
     * @param {*} context 
     */
    async getEntity(name, options, txn, context) {
        const entities = await this.uda.find({
            name,
            ...options,
            txn,
        });

        const triggers = this.gtMap.get(name);
        for (let entity in entities) {
            const validTriggers = triggers && triggers.filter(
                ele => (!ele.valueCheck || ele.valueCheck(entity))
            );
    
            if (validTriggers) {
                for (let trigger in validTriggers) {
                    await execTrigger.call(this, trigger, entity, txn, null, context);
                }
            }
        }
        return entities;
    }

    /**
     * findById带触发器
     * @param {*} name 
     * @param {*} projection 
     * @param {*} id 
     * @param {*} txn 
     * @param {*} context 
     */
    async getEntityByIds(name, projection, id, txn, context) {
        const entity = await this.uda.findById({
            name,
            projection,
            id,
            txn,
        });

        const triggers = this.gtMap.get(name);
        const validTriggers = triggers && triggers.filter(
            ele => (!ele.valueCheck || ele.valueCheck(entity))
        );

        if (validTriggers) {
            for (let trigger in validTriggers) {
                await execTrigger.call(this, trigger, enttiy, txn, null, context);
            }
        }

        return entity;
    }

    /**
     * 插入一个对象
     * @param name
     * @param data
     * @param txn
     * @returns {*}
     */
    insertEntity(name, data, txn, context) {
        assert(txn);
        const me = this;
        const allTriggers = this.itMap.get(name) && this.itMap.get(name).filter(
                (ele) => (!ele.valueCheck || ele.valueCheck(data))
            );

        let entity2 = assign({}, data);
        const beforeTriggers = allTriggers && allTriggers.filter(
                ele => ele.beforeAction
            );
        const afterTriggers = allTriggers && allTriggers.filter(
                ele => !ele.beforeAction
            );
        const insertTriggers = allTriggers && allTriggers;
        const volatileTriggers = afterTriggers && afterTriggers.filter(
                (ele) => ele.volatile
            );

        if (volatileTriggers && volatileTriggers.length > 0) {
            volatileTriggers.forEach(
                (volatileTrigger) => {
                    // dtLocaAttr未必一定要有，如果不需要保证最终一致性，则可以不传
                    // 如果有此属性，则需要靠这个域来保持最终一致性
                    if (volatileTrigger.dtLocalAttr) {
                        if (!entity2[volatileTrigger.dtLocalAttr]) {
                            entity2[volatileTrigger.dtLocalAttr] = [];
                        }
                        entity2.txnState = 1;
                        entity2[volatileTrigger.dtLocalAttr].push(
                            new ObjectID().toString().concat(volatileTrigger.id));
                    }
                }
            );
        }

        const execInsertTrigger = (triggers, data) => {
            const promises = triggers && triggers.map(
                    ele => ({
                        fn: execTrigger,
                        me,
                        params: [ele, data, txn, null, context],
                    })
                );

            return PromisesWithSerial(promises);
        };

        return execInsertTrigger(beforeTriggers, entity2)
            .then(
                () => this.uda.insert({
                    name: name, data: entity2, txn
                })
            )
            .then(
                (inserted) => execInsertTrigger(afterTriggers, inserted)
                    .then(
                        () => inserted
                    )
            );

    }

    /**
     * 更新一个对象
     * @param name
     * @param data
     * @param entityOrId
     * @param txn
     * @returns {*}
     */
    updateEntity(name, data, entityOrId, txn, context) {
        assert(txn);
        const me = this;
        const entityMap = this.utMap.get(name);
        const triggersArray = entityMap && keys(data).map(
                (ele) => {
                    const attrTriggers = entityMap.get(ele);
                    return attrTriggers || [];
                }
            );

        const updateInner = (entity2) => {
            // 触发器要满足value变化条件
            const updateTriggers = triggersArray && flatten(triggersArray).filter(
                    (ele) => (!ele.valueCheck || ele.valueCheck(entity2, data))
                );
            const beforeTriggers = updateTriggers && updateTriggers.filter(
                    ele => ele.beforeAction
                );
            const afterTriggers = updateTriggers && updateTriggers.filter(
                    ele => !ele.beforeAction
                );
            const volatileTriggers = afterTriggers && afterTriggers.filter(
                    (ele) =>ele.volatile
                );

            const data2 = assign({}, data);
            if (volatileTriggers && volatileTriggers.length > 0) {
                volatileTriggers.forEach(
                    (volatileTrigger) => {
                        if (volatileTrigger.dtLocalAttr) {
                            if (!entity2.hasOwnProperty(volatileTrigger.dtLocalAttr)) {
                                console.error(JSON.stringify(volatileTrigger));
                                throw new Error(`跨源事务，必须传入${volatileTrigger.dtLocalAttr}字段`);
                            }
                            if (!data2[volatileTrigger.dtLocalAttr]) {
                                data2[volatileTrigger.dtLocalAttr] = [];
                            }
                            //  todo    这里这种做法并不安全，并发情况下会出错
                            //  稍微优化，push之前先检查一遍
                            let uuid = new ObjectID().toString().concat(volatileTrigger.id);
                            if (!data2[volatileTrigger.dtLocalAttr].find((ele)=>ele === uuid)) {
                                data2.txnState = 1;
                                data2[volatileTrigger.dtLocalAttr].push(uuid);
                            }
                        }
                    }
                );
            }

            //  todo    martian-data不支持$inc算子和其他属性的一次性更新，暂时分两步
            const updateInner = () => {
                if (data2.$inc) {
                    return this.uda.updateOneById({
                        name,
                        data: pick(data2, "$inc"),
                        id: entity2.id,
                        txn,
                    }).then(
                        (updated1)=> {
                            return this.uda.updateOneById({
                                name,
                                data: omit(data2, "$inc"),
                                id: entity2.id,
                                txn,
                            }).then(
                                (updated2) => assign({}, updated1, updated2)
                            );
                        });
                }
                return this.uda.updateOneById({
                    name,
                    data: data2,
                    id: entity2.id,
                    txn,
                });
            };

            /**
             * 这里因为历史原因，对于更新前的触发器，调用参数分别是(data, txn, preEntity)，对更新后的触发器，调用参数分别是(entity, txn, preEntity)
             * data是更新数据，preEntity是更新前的数据，entity是更新后的数据
             * @param triggers
             * @param dataOrEntity
             * @param preEntity
             */
            const execUpdateTriggers = (triggers, dataOrEntity, preEntity) => {
                const promises = triggers && triggers.map(
                        ele => ({
                            fn: execTrigger,
                            me,
                            params: [ele, dataOrEntity, txn, preEntity, context],
                        })
                    );
                return PromisesWithSerial(promises);
            };

            return execUpdateTriggers(beforeTriggers, data2, entity2)
                .then(
                    () => updateInner()
                )
                .then(
                    (updated) => {
                        const afterEntity = assign({}, entity2, data2);
                        return execUpdateTriggers(afterTriggers, afterEntity, entity2)
                            .then(
                                () => updated
                            );
                    }
                );
        };

        if (typeof entityOrId === 'object') {
            return updateInner(entityOrId);
        }
        assert(typeof entityOrId === 'number');
        return this.uda.findById({
                name: name, id: entityOrId, txn
            })
            .then(
                (entity) => {
                    if (!entity) {
                        console.error("***********************************************");
                        console.error("触发触发器的时候，执行中的数据已被删除。");
                        console.error("***********************************************");
                        return Promise.resolve();
                    }
                    return updateInner(entity);
                }
            );
    }

    removeEntity(name, id, entity, txn, context) {
        assert(txn);
        const me = this;
        const entityMap = this.rtMap.get(name);
        const allTriggers = entityMap && this.rtMap.get(name).filter(
                (ele) => (!ele.valueCheck || ele.valueCheck(entity))
            );

        const deleteInner = (entity2) => {
            const removeTriggers = allTriggers;
            const beforeTriggers = removeTriggers && removeTriggers.filter(
                    ele => ele.beforeAction
                );
            const afterTriggers = removeTriggers && removeTriggers.filter(
                    ele => !ele.beforeAction
                );
            const volatileTriggers = afterTriggers && afterTriggers.filter(
                    (ele) => ele.volatile
                );
            if (volatileTriggers && volatileTriggers.length > 0) {
                // const triggersBySameDtr = values(groupBy(volatileTriggers.filter((ele)=>ele.dtLocalAttr), (ele)=>ele.dtLocalAttr));
                // if (triggersBySameDtr.some((triggersGourpByDtr)=>triggersGourpByDtr.length >= 2)) {
                //     console.warn(JSON.stringify(triggersBySameDtr.filter((ele)=>ele.length >= 2)[0]) + "等多个不稳定触发器共用了同一个dtLocalAttr域");
                //     throw new Error("多个不稳定的触发器无法共用一个dtLocalAttr域")
                // }
                volatileTriggers.forEach(
                    (volatileTrigger) => {
                        if (volatileTrigger.dtLocalAttr) {
                            if (!entity2[volatileTrigger.dtLocalAttr]) {
                                entity2[volatileTrigger.dtLocalAttr] = [];
                            }
                            let uuid = new ObjectID().toString().concat(volatileTrigger.id);
                            if (!entity2[volatileTrigger.dtLocalAttr].find((ele)=>ele === uuid)) {
                                entity2.txnState = 1;
                                entity2[volatileTrigger.dtLocalAttr].push(uuid);
                            }
                        }
                    }
                );
            }

            const execRemoveTriggers = (triggers, entity) => {
                const promises = triggers && triggers.map(
                        ele => ({
                            fn: execTrigger,
                            me,
                            params: [ele, assign({}, entity2), txn, null, context],
                        })
                    );
                return PromisesWithSerial(promises);
            };


            return execRemoveTriggers(beforeTriggers, entity2)
                .then(
                    () => this.uda.removeOneById({
                        name: name, id: entity2.id, txn
                    })
                ).then(
                (deleted) => execRemoveTriggers(afterTriggers, entity2)
                    .then(
                        () => deleted
                    )
            );
           /* return this.uda.removeOneById({
                    name: name, id: entity2.id, txn
                })
                .then(
                    (updated) => {
                        if (removeTriggers && removeTriggers.length > 0) {
                            const promises = [];
                            removeTriggers.forEach(
                                (ele) => {
                                    let funMethod = {};
                                    // console.log(`触发器【${ele.name}】开始`);
                                    funMethod.fn = execTrigger;
                                    funMethod.me = me;
                                    funMethod.params = [ele, assign({}, entity2), txn, context];
                                    promises.push(
                                        funMethod
                                    );
                                }
                            );
                            return PromisesWithSerial(promises)
                                .then(
                                    () => Promise.resolve(updated)
                                )
                        }
                        return Promise.resolve(updated);
                    }
                );*/
        };

        assert(typeof id === 'number');
        return this.uda.findById({
                name: name, id, txn
            })
            .then(
                (entity) => {
                    if (!entity) {
                        console.error("***********************************************");
                        console.error("触发触发器的时候，执行中的数据已被删除。");
                        console.error("***********************************************");
                        return Promise.resolve();
                    }
                    return deleteInner(entity);
                }
            );
    }

    doPatrol() {
        const me = this;
        const now = Date.now();
        const vtPromise = Promise.every(me.volatileArray.map(
            (trigger) => {
                const checkTriggerInner = (indexFrom) => {
                    // 寻找dtLocalAttr不为NULL的行
                    return me.uda.find({
                            name: trigger.entity,
                            query: {
                                txnState: {
                                    $eq: 1
                                },
                            }, indexFrom, count: ITER_COUNT
                        })
                        .then(
                            (list) => {
                                return Promise.every(
                                    list.map(
                                        (ele) => {
                                            assert(ele[trigger.dtLocalAttr].length >= 1);
                                            const dtLocalAttr = ele[trigger.dtLocalAttr].find((dtr)=>dtr.endsWith(trigger.id));
                                            // 对每一行，执行使之完整的操作
                                            // 判断是否当前trigger留下的不完整
                                            // 增加一个判断，只有当分布式事务的时间超过当前时间1分钟以上，才执行修补动作，这样可以避免一些重复的工作
                                            // const timeTriggerExec = parseInt(dtLocalAttr.substr(0, 8), 16);
                                            if (dtLocalAttr && now - (parseInt(dtLocalAttr.substr(0, 8), 16) * 1000) > me.volatileTriggerLatency) {

                                                return doVolatileLogic.call(this, trigger, ele);
                                            }
                                            return Promise.resolve(0);
                                        }
                                    )
                                ).then(
                                    () => {
                                        // let managedItems = 0;
                                        // result.forEach(
                                        //     (count) => managedItems += count
                                        // );
                                        // if (list.length === ITER_COUNT) {
                                        //     return checkTriggerInner(indexFrom + ITER_COUNT)
                                        //         .then(
                                        //             (count2) => Promise.resolve(count2 + managedItems)
                                        //         );
                                        // }
                                        return Promise.resolve(list.length);
                                    }
                                )
                            }
                        )
                };
                return checkTriggerInner(0)
                    .then(
                        (count) => {
                            if (count > 0) {
                                console.log(`状态触发器【${trigger.name}】恢复了【${count}】条数据到一致状态`);
                            }
                            return Promise.resolve(count);
                        }
                    ).catch(
                        (err) => {
                            console.error(`状态触发器【${trigger.name}】发生异常，异常信息是：`);
                            console.error(err);
                            throw err;
                        }
                    );
            }
        ));
        /*
         1、watcher没有跑过
         2、watcher跑了，并且不在冷却时间内
         */
        const noIntervalWt = me.wtArray.filter((ele) => !ele.executeInterval);
        const intervalWt = me.wtArray.filter((ele) => ele.executeInterval).filter(
            (ele) => {
                if (!ele.executeAt) {
                    ele.executeAt = Date.now();
                    return true;
                }
                return ele.executeAt < Date.now() - ele.executeInterval;
            });
        const finalWt = noIntervalWt.concat(intervalWt).filter((ele) => !ele.gyroscope);

        /*  todo    这个机制先保留
         *  所有的watcher执行Promise.every，太猛了一点，至少保证相同entity的串行执行
         */
        const isGroupEntity = (entity) => {
            if (["user", "externalCredit"].includes(entity)) {
                return "userCredit";
            }
            else {
                return entity;
            }
        };
        const ppp = groupBy(finalWt, (ele) => isGroupEntity(ele.entity));
        const wtPromise = values(groupBy(finalWt, (ele) => isGroupEntity(ele.entity))).map(
            (entityWts) => {
                return PromisesWithSerial(entityWts.map(
                    (watcher) => {
                        let funMethod = {};
                        // console.log(`触发器【${ele.name}】开始`);
                        funMethod.fn = execWatcher;
                        funMethod.me = me;
                        funMethod.params = [watcher];
                        return funMethod;
                    }
                ));
            }
        );

        // const wtPromise = Promise.every(finalWt.map(
        //     (watcher) => execWatcher.call(me, watcher)
        // ));

        return Promise.every(wtPromise.concat(vtPromise))
            .then(
                (result)=> {
                    const result2 = flatten(result);
                    return result2;
                }
            );
        // return Promise.every([vtPromise, wtPromise]);
    }

    callWatchers(watcherIds, entityId) {
        const me = this;
        const watchers = watcherIds.map(
            (id) => me.wtMap.get(id)
        ).filter((ele) => ele);
        return Promise.all(
            watchers.map(
                (watcher) => execWatcher.call(me, watcher, entityId)
            )
        );
    }

    setVolatileTriggerLatency(latency) {
        this.volatileTriggerLatency = latency;
    }

    // setExecPoolLantency(latency) {
    //     this.execPoolLantency = latency;
    // }
    //
    // execPoolRun() {
    //     if (this.execPool.size != 0) {
    //         let itemList = [];
    //         for (let item of this.execPool.values()) {
    //             console.log(JSON.stringify(item));
    //             itemList.push(item);
    //         }
    //         console.log(`运行池中还有【${this.execPool.size}】个事件没有运行`);
    //         console.log(`运行池中还有【${itemList.filter((item)=>item.injectedAt <= Date.now() - this.execPoolLantency).length}】个事件在等待运行`);
    //         const iterator = (idx)=> {
    //             if (idx === itemList.length) {
    //                 return Promise.resolve();
    //             }
    //             //  运行超过了注入时间的exec
    //             if (itemList[idx].injectedAt <= Date.now() - this.execPoolLantency) {
    //                 return itemList[idx].func.apply(this, itemList[idx].params)
    //                     .then(
    //                         ()=> {
    //                             //  运行结束，删除
    //                             for (let key of this.execPool.keys()) {
    //                                 if (this.execPool.get(key) === itemList[idx]) {
    //                                     this.execPool.delete(key);
    //                                     console.log(`运行池完成了一个事件的运行并且删除`);
    //                                     break;
    //                                 }
    //                             }
    //                             //  若是volatile的触发器，还得置空dtLocalAttr
    //                             if (itemList[idx].trigger.volatile) {
    //                                 return Promise.all(
    //                                     itemList[idx].entities.map(
    //                                         (entity)=> {
    //                                             return emptyDtLocalAttr.call(this, itemList[idx].trigger, entity, null)
    //                                         }
    //                                     )
    //                                     )
    //                                     .then(
    //                                         ()=> {
    //                                             return iterator(idx + 1);
    //                                         }
    //                                     )
    //                             }
    //                             return iterator(idx + 1);
    //                         },
    //                         (err)=> {
    //                             throw err;
    //                         }
    //                     ).catch(
    //                         (err)=> {
    //                             throw err;
    //                         }
    //                     )
    //             }
    //             return iterator(idx + 1);
    //         };
    //         try {
    //             return iterator(0)
    //                 .then(
    //                     ()=> {
    //                         return Promise.resolve();
    //                     }
    //                 )
    //                 .catch(
    //                     (err)=> {
    //                         console.error("运行池中事件运行失败，原因是：");
    //                         console.error(err);
    //                         throw err;
    //                     }
    //                 )
    //         }
    //         catch (err) {
    //             console.error("运行池中事件运行失败，原因是：");
    //             console.error(err);
    //             throw err;
    //         }
    //
    //     }
    //     return Promise.resolve();
    // }
}

module.exports = SystemWarden;
