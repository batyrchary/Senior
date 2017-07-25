const { connect } = require('mongodb').MongoClient;
const config = require('./config');
const { resolve } = require('path');

// retrieve all data
var displayForm = function(res) { 
  const data = [];
  console.log(config);

  connect(config.dburl, function(err, db) {
    const forms = db.collection(config.collection).find();

    forms.each(function(err, doc) {
      if(doc) {
        data.push(doc);
        return;
      }

      res.render('index', { data: data });
    });
  });
}

module.exports = displayForm;
