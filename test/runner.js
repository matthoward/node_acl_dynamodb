const dynamodbBackend = require('../'),
    tests = require('../node_modules/acl/test/tests'),
    AWS = require('aws-sdk');

const db = new AWS.DynamoDB({
    endpoint: new AWS.Endpoint("http://localhost:8000"),
    accessKeyId: "myKeyId",
    secretAccessKey: "secretKey",
    region: "us-east-1",
    apiVersion: "2016-01-07"
});



describe('DynamoDB - Default', function() {
    before(function(done) {
        var self = this;
        self.backend = new dynamodbBackend(db, "acl_default_");

        db.listTables({}, function(err, data) {
            data.TableNames.forEach(tbl => {
                console.log(`deleting table ${tbl}...`);
                db.deleteTable(tbl);
            });
            done();
        });
    });

    run();
});


describe('DynamoDB - useSingle', function() {
    before(function(done) {
        var self = this;
        self.backend = new dynamodbBackend(db, "acl_single_", true);
        done();
    });

    run();
});

function run() {
    Object.keys(tests).forEach(function(test) {
        tests[test]();
    });
}
