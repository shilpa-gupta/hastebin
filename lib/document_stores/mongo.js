

var MongoClient = require('mongodb').MongoClient,
    winston = require('winston');

var MongoDocumentStore = function (options) {
    this.expire = options.expire;
    this.connectionUrl = process.env.DATABASE_URl || options.connectionUrl;
};

MongoDocumentStore.prototype.set = function (key, data, callback, skipExpire) {
    var now = Math.floor(new Date().getTime() / 1000),
        that = this;

    this.safeConnect(function (err, db) {
        if (err)
            return callback(false);
        
        db.collection('entries').insert({
            'entry_id': key,
            'value': data,
            'expiration': that.expire && !skipExpire ? now : -1
        }, {
            upsert: true
        }, function (err, existing) {
            if (err) {
                winston.error('error persisting value to mongodb', { error: err });
                return callback(false);
            }

            callback(true);
        });
    });
};

MongoDocumentStore.prototype.get = function (key, callback, skipExpire) {
    var now = Math.floor(new Date().getTime() / 1000),
        that = this;

    this.safeConnect(function (err, db) {
        if (err)
            return callback(false);
        
        db.collection('entries').findOne({
            'entry_id': key,
            $or: [
                { expiration: -1 },
                { expiration: { $gt: now } }
            ]
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
            if (this.expire && !skipExpire) {
                db.createIndex({ "expiration": 1 }, { expireAfterSeconds: this.expire })
            }
            callback(undefined, db);
        }
    });
};

module.exports = MongoDocumentStore;
