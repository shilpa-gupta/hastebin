

var MongoClient = require('mongodb').MongoClient,
    winston = require('winston');

var MongoDocumentStore = function (options) {
    this.connectionUrl = process.env.DATABASE_URl || options.connectionUrl;
};

MongoDocumentStore.prototype.set = function (key, data, callback, skipExpire) {

    this.safeConnect(function (err, db) {
        if (err)
            return callback(false);
        db.collection('entries').insert({
            'entry_id': key,
            'value': data,
            'expiration': !skipExpire ? {type: Date, expires: '2m',default: Date.now}  : -1
        }, {w: 1}, function (err, existing) {
            if (err) {
                winston.error('error persisting value to mongodb', { error: err });
                return callback(false);
            }

            callback(true);
        });
    });
};

MongoDocumentStore.prototype.get = function (key, callback) {
    this.safeConnect(function (err, db) {
        if (err)
            return callback(false);
        
        db.collection('entries').findOne({
            'entry_id': key
        }, function (err, entry) {
            if (err) {
                winston.error('error persisting value to mongodb', { error: err });
                return callback(false);
            }
            callback(entry === null ? false : entry.value);
        });
    });
};

MongoDocumentStore.prototype.safeConnect = function (callback) {
    MongoClient.connect(this.connectionUrl, function (err, client) {
        if (err) {
            winston.error('error connecting to mongodb', { error: err });
            callback(err);
        } else {
            var db = client.db('truera-paste')
            callback(undefined, db);
        }
    });
};

module.exports = MongoDocumentStore;
