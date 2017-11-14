var dynamodbBackend = require('../'),
    tests = require('../node_modules/acl/test/tests');

describe('DynamoDB - Default', function() {
    before(function(done) {
        var self = this,
            AWS = require('aws-sdk'),
            db = new AWS.DynamoDB({
                endpoint: new AWS.Endpoint("http://localhost:8000"),
                accessKeyId: "myKeyId",
                secretAccessKey: "secretKey",
                region: "us-east-1",
                apiVersion: "2016-01-07"
            });
        self.backend = new dynamodbBackend(db, "acl_default");
        done();
    });

    run();
});


describe('DynamoDB - useSingle', function() {
    before(function(done) {
        var self = this,
            AWS = require('aws-sdk'),
            db = new AWS.DynamoDB({
                endpoint: new AWS.Endpoint("http://localhost:8000"),
                accessKeyId: "myKeyId",
                secretAccessKey: "secretKey",
                region: "us-east-1",
                apiVersion: "2016-01-07"
            });
        self.backend = new dynamodbBackend(db, "acl_single", true);
        done();
    });

    run();
});

function run() {
    Object.keys(tests).forEach(function(test) {
        tests[test]();
    });
}
