CREATE KEYSPACE music WITH REPLICATION = {
     'class' : 'SimpleStrategy',
     'replication_factor' : 1
};

CREATE TABLE music.library (
    number int, name text,
    artist text, album text,
    genres set<text>, duration int,
    released date,
PRIMARY KEY ((artist), album, number))
WITH CLUSTERING ORDER BY (album ASC, number ASC);

# MONGO
db.createUser(
   {
     user: "data",
     pwd: "password",
     roles:
       [
         { role: "readWrite", db: "library" },
         { role: "readWrite", db: "user_listens" },
       ]
   }
)

mongoimport --jsonArray --db test --collection library \
          --authenticationDatabase admin --username data --password password \
          --file data.json