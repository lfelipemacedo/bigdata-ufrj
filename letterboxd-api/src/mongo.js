
const mongoose = require('mongoose');

mongoose.connect('mongodb://engsoft_ufrj:123456789ES@ac-mjy99vr-shard-00-00.dhzryvz.mongodb.net:27017,ac-mjy99vr-shard-00-01.dhzryvz.mongodb.net:27017,ac-mjy99vr-shard-00-02.dhzryvz.mongodb.net:27017/?ssl=true&replicaSet=atlas-pz2lem-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0', {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

const db = mongoose.connection;

module.exports = db;