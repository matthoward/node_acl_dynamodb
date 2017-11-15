"user strict";
var contract = require('acl/lib/contract');
var lodash = require('lodash');
var aclTableName = 'resources'; 
var AWS = require('aws-sdk');
var async = require('async');

// Name of the table where meta and allowsXXX are stored.
// If prefix is specified, it will be prepended to this name, like acl_resources
function DynamoDBBackend(db, prefix, useSingleTable, readCapacityUnits, writeCapacityUnits) {
    this.db = db;
    this.client = new AWS.DynamoDB.DocumentClient({
        service: db
    });
    this.prefix = typeof prefix !== 'undefined' ? prefix : '';
    this.useSingleTable = (typeof useSingleTable !== 'undefined') ? useSingleTable : false;
    this.readCapacityUnits = (typeof readCapacityUnits !== 'undefined') ? readCapacityUnits : 5;
    this.writeCapacityUnits = (typeof writeCapacityUnits !== 'undefined') ? writeCapacityUnits : 5;
    this.tables = [];
    this.aclTableName = aclTableName;
}

DynamoDBBackend.prototype = {
    /**
        Begins a transaction.
    */
    begin: function() {
        // returns a transaction object
        return [];
    },

    /**
        Ends a transaction (and executes it)
    */
    end: function(transaction, cb) {
        contract(arguments).params('array', 'function').end();
        // Execute transaction
        async.series(transaction, function(err) {
            // console.log(err);
            cb(err instanceof Error ? err : undefined);
        });
    },

    /**
        Cleans the whole storage.
    */
    clean: function(cb) {
        contract(arguments).params('function').end();
        this.tables.forEach(function(table) {
            this.db.deleteTable({
                TableName: table
            }).send();
        });
    },

    /**
        Gets the contents at the bucket's key.
    */
    get: function(bucket, key, cb) {
        contract(arguments)
            .params('string', 'string|number', 'function')
            .end();

        key = encodeText(key);
        var self = this;
        const { tableName, useBucketInKey } = getTableDefinition(self, bucket);
        var params = {
            TableName: tableName,
            Key: {
                key: key
            }
        };
        if (useBucketInKey) params.Key._bucketname = bucket;

        self.client.get(params, function(err, data) {
            if (err) return cb(err);
            if (!lodash.isObject(data) || !lodash.isObject(data.Item)) return cb(undefined, []);
            var item = data.Item;
            return cb(undefined, lodash.without(lodash.keys(item), "key", "_id", "_bucketname"));
        });

    },

    /**
    	Returns the union of the values in the given keys.
    */
    union: function(bucket, keys, cb) {
        contract(arguments)
            .params('string', 'array', 'function')
            .end();

        var self = this;
        keys = encodeAll(keys);
        var params = {
            RequestItems: {}
        };
        const { tableName, useBucketInKey } = getTableDefinition(self, bucket);
        params.RequestItems[tableName] = {
            Keys: []
        };
        params.RequestItems[tableName].Keys = lodash.map(keys, function(key) {
            return {
                key: key
            };
        });
        if (useBucketInKey)
            params.RequestItems[tableName].Keys = lodash.map(params.RequestItems[tableName].Keys, function(o) {
                o._bucketname = bucket;
                return o;
            });

        var result = {};
        result[tableName] = [];

        function tryUntilSuccess(params, result, retryCount, innercb) {
            //console.log(util.inspect(params, false, 5));
            self.client.batchGet(params, function(err, data) {
                if (err) return innercb(err);

                if (lodash.isObject(data) && lodash.isObject(data.Responses) && lodash.isObject(data.Responses[tableName])) {
                    result[tableName] = result[tableName].concat(data.Responses[tableName]);
                }

                if (lodash.isObject(data) && lodash.isObject(data.UnprocessedKeys) && !lodash.isEmpty(data.UnprocessedKeys)) {
                    retryCount += 1;
                    var nextParams = {
                        RequestItems: data.UnprocessedKeys
                    };
                    if ('ReturnConsumedCapacity' in params) nextParams.ReturnConsumedCapacity = params.ReturnConsumedCapacity;
                    return setTimeout(function() {
                        tryUntilSuccess(nextParams, result, retryCount, innercb);
                    }, Math.pow(retryCount, 2) * 1000);
                } else {
                    //console.log(util.inspect(result, false, 5));
                    result = result[tableName];
                    var keyArrays = [];
                    result.forEach(function(item) {
                        keyArrays.push.apply(keyArrays, lodash.keys(item));
                    });
                    return innercb(undefined, lodash.without(lodash.union(keyArrays), "key", "_id", "_bucketname"));
                }
            });
        }

        tryUntilSuccess(params, result, 0, cb);
    },

    /**
    	   Adds values to a given key inside a bucket.
    	*/
    add: function(transaction, bucket, key, values) {
        contract(arguments)
            .params('array', 'string', 'string|number', 'string|array|number')
            .end();
        if (key === "key") throw new Error("Key name 'key' is not allowed.");
        var self = this;
        const { tableName, useBucketInKey } = getTableDefinition(self, bucket);
        if (!(tableName in self.tables)) {


            transaction.push(function(cb) {
                var params = {
                    TableName: tableName,
                    KeySchema: [{
                        AttributeName: "key",
                        KeyType: "HASH"
                    }],
                    AttributeDefinitions: [{
                        AttributeName: "key",
                        AttributeType: "S"
                    }],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: self.readCapacityUnits,
                        WriteCapacityUnits: self.writeCapacityUnits
                    }
                };

                if (useBucketInKey) {
                    params.KeySchema.push({
                        AttributeName: "_bucketname",
                        KeyType: "RANGE"
                    });
                    params.AttributeDefinitions.push({
                        AttributeName: "_bucketname",
                        AttributeType: "S"
                    });
                }

                self.tables.push(tableName);
                //console.log(`creating table ${tableName}`);
                self.db.createTable(params, function(err, data) {
                    // Don't care about Errors or response data
                    cb(undefined);
                });

            });


        }
        var params = {
            TableName: tableName,
            Key: {
                key: encodeText(key)
            },
            UpdateExpression: 'SET '
        };
        if (useBucketInKey) params.Key._bucketname = bucket;

        values = makeArray(values);
        var names = values.reduce(function (result, value, idx) {
            result['#key' + idx] = encodeText(value);
            return result;
        }, {});
        params.UpdateExpression += Object.keys(names).map((key) => key + ' = :trueVal').join(', ');
        params.ExpressionAttributeValues = {
            ':trueVal': true
        };

        params.ExpressionAttributeNames = names;
        transaction.push(function(cb) {
            self.client.update(params, function(err, data) {
                if (err instanceof Error) return cb(err);
                return cb(undefined);
            });
        });
    },

    /**
        Delete the given key(s) at the bucket
    */
    del: function(transaction, bucket, keys) {
        contract(arguments)
            .params('array', 'string', 'string|array')
            .end();

        keys = makeArray(keys);
        var self = this;
        const { tableName, useBucketInKey } = getTableDefinition(self, bucket);
        var params = {
            RequestItems: {}
        };
        params.RequestItems[tableName] = [];
        keys.forEach(function(key) {
            var delRequest = {
                DeleteRequest: {
                    Key: {
                        key: encodeText(key)
                    }
                }
            };
            if (useBucketInKey) delRequest.DeleteRequest.Key._bucketname = bucket;
            params.RequestItems[tableName].push(delRequest);
        });
        transaction.push(function(cb) {
            self.client.batchWrite(params, function(err, data) {
                if (err instanceof Error) return cb(err);
                return cb(undefined);
            });
        });
    },

    /**
    	Removes values from a given key inside a bucket.
    */
    remove: function(transaction, bucket, key, values) {
        contract(arguments)
            .params('array', 'string', 'string|number', 'string|array|number')
            .end();

        var self = this;
        const { tableName, useBucketInKey } = getTableDefinition(self, bucket);
        var params = {
            TableName: tableName,
            Key: {
                key: encodeText(key)
            },
            UpdateExpression: 'REMOVE '
        };
        if (useBucketInKey) params.Key._bucketname = bucket;

        values = makeArray(values);
        var names = values.reduce(function (result, value, idx) {
            result['#key' + idx] = encodeText(value);
            return result;
        }, {});
        params.UpdateExpression += Object.keys(names).join(', ');

        params.ExpressionAttributeNames = names;
        transaction.push(function(cb) {
            self.client.update(params, function(err, data) {
                if (err instanceof Error) return cb(err);
                return cb(undefined);
            });
        });
    }
};

function encodeText(text) {
    return text.toString();
}

function encodeAll(arr) {
    if (!Array.isArray(arr)) {
        return encodeText(arr);
    }
    return arr.map(val => encodeText(val));
}

function makeArray(arr) {
    return Array.isArray(arr) ? encodeAll(arr) : encodeAll([arr]);
}

function cleanInvalidTableNameChars(bucket) {
    return bucket.replace(/[^A-Za-z0-9_\-\.]/g,'');
}

function getTableDefinition(dynamoDBBackend, bucket) {
    if (dynamoDBBackend.useSingleTable) {
        return {
            useBucketInKey: true,
            tableName: dynamoDBBackend.prefix + dynamoDBBackend.aclTableName
        };
    }
    if (bucket.startsWith("allows")) {
        return {
            useBucketInKey: true,
            tableName: dynamoDBBackend.prefix + "allows"
        };
    }
    return {
        useBucketInKey: false,
        tableName: dynamoDBBackend.prefix + cleanInvalidTableNameChars(bucket)
    };
}

exports = module.exports = DynamoDBBackend;
