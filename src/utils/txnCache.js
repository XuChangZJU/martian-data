const cloneDeep = require('lodash/cloneDeep');
const merge = require('lodash/merge');
const unset = require('lodash/unset');

/**
 * 判断数据是否满足完整的投影
 * @param {*} projection 
 * @param {*} data 
 * @return 投影后的数据
 */
function projectionConcluded(projection, data) {
    // 要判断需要的projection是否全部在内
    const projected = {};
    for (let attr in projection) {
        if (attr.toLowerCase().startsWith('$fncall')) {
            return;
            /* const { $as }  = projection[attr];
            if (!data.hasOwnProperty($as)) {
                conclude = false;
                break;
            }
            else {
                Object.assign(projected, {
                    [$as]: data[$as],
                });
            } */
        }
        else {
            const value = projection[attr];
            if (typeof value === 'object') {
                // 有对象连接就不能用cache，因为无法保证数据一致性
                return;
            }
            else if (typeof value === 'string') {
                if (!data.hasOwnProperty(value)) {
                    return;
                }
                else {
                    Object.assign(projected, {
                        [value]: data[value],
                    });
                }
            }
            else {
                if (!data.hasOwnProperty(attr)) {
                    return;
                }
                else {
                    Object.assign(projected, {
                        [attr]: data[attr],
                    });
                }
            }
        }
    }
    return projected;
}

class txnCache {
    constructor() {
        this.entities = {};
    }

    save(entity, data) {
        const { id } = data;
        if (id) {
            const { [entity]: entitySubTree } = this.entities;
            if (entitySubTree) {
                if (entitySubTree.hasOwnProperty(id)) {
                    const leaf = entitySubTree[id];
                    merge(leaf, cloneDeep(data));
                }
                else {
                    Object.assign(entitySubTree, {
                        [id]: cloneDeep(data),
                    });
                }
            }
            else {
                const entitySubTree = {
                    [id]: cloneDeep(data),
                };
                Object.assign(this.entities, {
                    [entity]: entitySubTree,
                });
            }
        }
    }

    load(entity, projection, id) {
        const { [entity]: entitySubTree } = this.entities;
        if (entitySubTree) {
            if (entitySubTree.hasOwnProperty(id)) {
                const data = entitySubTree[id];

                const result = projectionConcluded(projection, data);
                return result;
            }
        }
    }

    clear(entity, id) {
        const { [entity]: entitySubTree } = this.entities;
        if (entitySubTree) {
            if (entitySubTree.hasOwnProperty(id)) {
                unset(entitySubTree, `${id}`);
            }
        }
    }

    clearEntity(entity) {
        if (this.entities.hasOwnProperty(entity)) {
            unset(this.entities, `${entity}`);
        }
    }
}

module.exports = txnCache;
