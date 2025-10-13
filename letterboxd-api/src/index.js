/*** DATABASE ***/
const { MongoClient } = require('mongodb');
require('dotenv').config();

MongoClient.connect(process.env.MONGO_DB || '', { useUnifiedTopology: true })
    .then(client => {
        db = client.db('letterboxd-analysis');
        app.listen(3000, () => console.log('MongoDB OK!'));
    })
    .catch(console.error);

const express = require('express');
const morgan = require('morgan');
const cors = require('cors');
const path = require('path');

/*** SERVER ***/
const app = express();
app.use(morgan('dev'));
app.use(express.json());
app.use(cors());

app.use(express.static(path.join(__dirname, '../public')));

app.get('/', (req, res) => {
    res.send('OK!');

    res.sendFile(path.join(__dirname, '../public/index.html'));
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