/*** DATABASE ***/
const { MongoClient } = require('mongodb');

MongoClient.connect('mongodb://engsoft_ufrj:123456789ES@ac-mjy99vr-shard-00-00.dhzryvz.mongodb.net:27017,ac-mjy99vr-shard-00-01.dhzryvz.mongodb.net:27017,ac-mjy99vr-shard-00-02.dhzryvz.mongodb.net:27017/?ssl=true&replicaSet=atlas-pz2lem-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0', { useUnifiedTopology: true })
    .then(client => {
        db = client.db('letterboxd-analysis');
        app.listen(3000, () => console.log('MongoDB OK!'));
    })
    .catch(console.error);

const express = require('express');
const cors = require('cors');

/*** SERVER ***/
const app = express();
app.use(express.json());
app.use(cors());

app.get('/', (req, res) => {
    res.send('OK!');
});

app.get('/avg-rating-by-country', async (req, res) => {
    const data = await db.collection('avg_rating_by_country').find().limit(100).toArray();
    res.json(data);
});

app.get('/avg-rating-by-year', async (req, res) => {
    const data = await db.collection('avg_rating_by_year').find().limit(100).toArray();
    res.json(data);
});

app.get('/genres-by-country', async (req, res) => {
    const data = await db.collection('genres_by_country').find().limit(100).toArray();
    res.json(data);
});

app.get('/movies-by-genre-and-year', async (req, res) => {
    const data = await db.collection('movies_by_genre_and_year').find().limit(100).toArray();
    res.json(data);
});

app.get('/releases-by-country', async (req, res) => {
    const data = await db.collection('releases_by_country').find().limit(100).toArray();
    res.json(data);
});

app.listen(3000, () => {
    console.log('Server OK!');
});