const express = require('express');
const snowflake = require('snowflake-sdk');

const app = express();
const port = 3000;

require('dotenv').config();

const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USER,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
});


// Vérifier la connexion à Snowflake
connection.connect((err, conn) => {
    if (err) {
        console.error('Erreur de connexion :', err.message);
        process.exit(1);
    } else {
        console.log('Connexion réussie à Snowflake');
    }
});

// Route pour obtenir les données depuis Snowflake
app.get('/api/products', (req, res) => {
    connection.execute({
        sqlText: 'SELECT * FROM cleaned_products_table LIMIT 10;',
        complete: (err, stmt, rows) => {
            if (err) {
                console.error('Erreur lors de l\'exécution :', err.message);
                res.status(500).send('Erreur lors de l\'exécution de la requête.');
            } else {
                res.json(rows); // Retourner les résultats en JSON
            }
        }
    });
});

// Lancer le serveur
app.listen(port, () => {
    console.log(`Serveur backend en écoute sur http://localhost:${port}`);
});
