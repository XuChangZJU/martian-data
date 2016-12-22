/**
 * Created by Administrator on 2016/6/20.
 */
"use strict";

const merge = require("lodash/merge");
const keys = require("lodash/keys");

require('isomorphic-fetch');

const constants = require('../constants');

function replicateExecTree(execTree) {
	let newTree = {};
	newTree.query = execTree.query;
	newTree.projection = execTree.projection;
	newTree.sort = execTree.sort;

	if(execTree.joins) {
		newTree.joins = [];
		execTree.joins.forEach(
			(ele) => {
				let ele2 = merge({}, ele);
				ele2.node = replicateExecTree(ele.node);
				newTree.joins.push(ele2);
			}
		);
	}

	return newTree;
}

class Remote {
	constructor(settings) {
		const apis = {};
		if (settings.hasOwnProperty('remoteApiRouter')) {
			keys(constants.defaultRemoteApis).forEach(
				(ele) => {
					apis[ele] = settings.remoteApiRouter.concat(constants.defaultRemoteApis[ele]);
				}
			);
		}
		else {
			keys(constants.defaultRemoteApis).forEach(
				(ele) => {
					apis[ele] = constants.defaultRemoteApiRouter.concat(constants.defaultRemoteApis[ele]);
				}
			);
		}
		let defaultSettings = {
			apis,
			resolveResponse: (res) => {
				return new Promise.resolve(res);
			}
		};
		this.settings = merge({}, defaultSettings, settings);
	}


	accessRemoteApi(url, init) {
		let init2;
		if(this.headers) {
			init2 = merge({}, init, {
				headers: this.headers
			});
		}
		else {
			init2 = init;
		}

		if(!(url.startsWith("http://" || url.startsWith("https://")))) {
			url = this.serverUrl.concat(url);
		}

		return fetch(url, init2)
			.then(
				(res) => {
					return this.settings.resolveResponse(res);
				},
				(err) => {
					return Promise.reject(err);
				}
			);
	}

	setHttpHeaders(obj) {
		this.headers = merge({}, this.headers, obj);
	}

	connect(url) {
		this.serverUrl = url;
		this.apis = {};
		for(let i in this.settings.apis) {
			this.apis[i] = url.concat(this.settings.apis[i]);
		}

		// 登录动作由外层来控制，以实现多进程共享访问令牌
		return Promise.resolve();
	}

	disconnect() {
		// 注销动作由外层来控制
		return Promise.resolve();
	}

	find(name, execTree, indexFrom, count, isCounting) {
		let body = {
			name,
			execTree: replicateExecTree(execTree),
			indexFrom,
			count,
			isCounting
		};
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body)
		}
		return this.accessRemoteApi(this.apis.urlFind, init);
	}


	insert(name, data, schema) {
		let body = {
			name,
			data
		};
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body)
		}
		return this.accessRemoteApi(this.apis.urlInsert, init);
	}

	update(name, updatePart, query) {
		let body = {
			name,
			updatePart,
			query
		};
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body)
		}
		return this.accessRemoteApi(this.apis.urlUpdate, init);
	}

	updateOneById(name, updatePart, id, schema) {
		let body = {
			name,
			updatePart,
			id
		};
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body)
		}
		return this.accessRemoteApi(this.apis.urlUpdateOneById, init);
	}

	remove(name, query) {
		let body = {
			name,
			query
		};
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body)
		}
		return this.accessRemoteApi(this.apis.urlRemove, init);
	}

	removeOneById(name, id) {
		let body = {
			name,
			id
		};
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body)
		}
		return this.accessRemoteApi(this.apis.urlRemoveOneById, init);
	}

	createSchema(schema, name) {
		if(this.apis.urlCreateSchema) {
			let body = {
				name,
				schema
			};
			let init = {
				method: "POST",
				headers: {
					"Content-type": "application/json"
				},
				body: JSON.stringify(body)
			}
			return this.accessRemoteApi(this.apis.urlCreateSchema, init);
		}
		else {
			// 一般不提供远程的创建表接口
			return Promise.resolve();
		}
	}

	dropSchema(name) {
		if(this.apis.urlDropSchema) {
			let body = {
				name
			};
			let init = {
				method: "POST",
				headers: {
					"Content-type": "application/json"
				},
				body: JSON.stringify(body)
			}
			return this.accessRemoteApi(this.apis.urlDropSchema, init);
		}
		else {
			// 一般不提供远程的创建表接口
			return Promise.resolve();
		}

	}

	getDefaultKeyType(name) {
		let body = {
			name
		};
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body)
		}
		return this.accessRemoteApi(this.apis.urlKeyType, init);

	}

	getDefaultKeyName(name) {
		let body = {
			name
		};
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			},
			body: JSON.stringify(body)
		};
		return this.accessRemoteApi(this.apis.urlKeyName, init);
	}

	getSchemas() {
		let init = {
			method: "POST",
			headers: {
				"Content-type": "application/json"
			}
		};
		return this.accessRemoteApi(this.apis.urlSchemas, init);
	}
}


module.exports = Remote;
