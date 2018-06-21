# Conversion Steps

1. Provision $80 Node.js Lightsail box as "log-manager-import" in us-east-1
2. ssh into box
  a. sudo apt-get install postgresql postgresql-contrib
  b. sudo -i -u postgres
  c. psql
  d. \password postgres (set to "postgres" too)
  e. \q
  f. createdb log_manager
  g. exit  (should now be "bitnami" again)
  h. sudo snap install heroku --classic
  i. heroku login (then login as your user)
  j. heroku pg:backups:capture --app cc-log-manager
  k. heroku pg:backups:download --app cc-log-manager
  l. pg_restore --verbose --clean --jobs=2 --no-acl --no-owner -h localhost -U postgres -d log_manager latest.dump
  m. WAIT A LONG TIME (1 HOUR?)
  n. git clone https://github.com/dougmartin/log-manager-import.git
  o. cd log-manager-import
  p. npm install
  q. create aws-config.json

