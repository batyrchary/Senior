const { Router } = require('express');
const { ObjectID, MongoClient } = require('mongodb');
const displayForm = require('./displayForm');
const { insertData, deleteData, updateData } = require('./controller');

var router = Router();

router
.get('/', function (req, res) {
  displayForm(res);
}).post('/', insertData)
.get('/test', function(req, res) {
	res.json('asdf');
})
.post('/delete', deleteData)
.post('/edit', updateData);

module.exports = router;
