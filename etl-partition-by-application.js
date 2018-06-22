const AWS = require('aws-sdk');
const zlib = require('zlib');
const async = require('async');

AWS.config.loadFromPath('./aws-config.json');

const s3 = new AWS.S3();

const listObjects = ({continuationToken, perObjectCallback}) => {
  const params = {
    Bucket: 'log-manager-data',
    Prefix: 'processed-logs/',
    MaxKeys: 1000
  };
  if (continuationToken) {
    params.ContinuationToken = continuationToken;
  }
  s3.listObjectsV2(params, (err, data) => {
    if (err) {
      console.error(err);
    }
    else {
      const next = () => {
        process.nextTick(() => {
          drainContents();
        });
      };
      const drainContents = () => {
        if (data.Contents.length > 0) {
          const object = data.Contents.shift();
          const params = {
            Bucket: 'log-manager-data',
            Key: object.Key
          };
          s3.getObject(params, (err, data) => {
            if (err) {
              console.error(err);
              next();
            }
            else {
              zlib.gunzip(data.Body, (err, body) => {
                if (err) {
                  console.log(err);
                  next();
                }
                else {
                  perObjectCallback({object, body, done: next});
                }
              });
            }
          });
        }
        else {
          if (data.NextContinuationToken) {
            process.nextTick(() => {
              listObjects({continuationToken: data.NextContinuationToken, perObjectCallback});
            });
          }
        }
      };
      drainContents();
    }
  });
};

const listLogEntries = ({body, perLogEntryCallback}) => {
  body.toString().split('\n').forEach((line) => {
    if (line.length > 0) {
      perLogEntryCallback(JSON.parse(line));
    }
  });
};

listObjects({perObjectCallback: ({object, body, done}) => {
  const applications = {};
  console.log(object.Key);
  listLogEntries({body, perLogEntryCallback: (entry) => {
    applications[entry.application] = applications[entry.application] || [];
    applications[entry.application].push(JSON.stringify(entry));
  }});
  Object.keys(applications).forEach((application) => {
    zlib.gzip(applications[application].join('\n'), (err, gzipped) => {
      if (err) {
        console.log(err);
        done();
      }
      else {
        const applicationKey = object.Key.replace(/^processed-logs\//, `per-application-logs/${application.replace(/\//g, '').trim()}/`);
        s3.upload({
          Bucket: 'log-manager-data',
          Key: applicationKey,
          Body: gzipped,
          ContentType: 'application/json',
          ContentEncoding: 'gzip',
        }, (err) => {
          if (err) {
            console.log(err);
          }
          done();
        });
      }
    });
  });
}});