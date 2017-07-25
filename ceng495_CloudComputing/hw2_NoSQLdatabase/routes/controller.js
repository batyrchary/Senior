const { ObjectID, MongoClient } = require('mongodb');
const config = require('./config');

module.exports.getData = function(req, res) {
  const data = [];

  MongoClient.connect(config.dburl, function(err, db) {
    const forms = db.collection(config.collection).find();

    forms.each(function(err, doc) {
      if(doc) {
        data.push(doc);
        return;
      }

      res.render('index', {data: data});
    });
  });
}

module.exports.insertData = function (req, res) {
  const { release_date, movie_name, tags, url, actors, ExtrafieldDefinition, ExtrafieldDescription } = req.body;
  const data = { release_date, movie_name, tags, url, actors };

  if (Array.isArray(ExtrafieldDefinition) && Array.isArray(ExtrafieldDescription)) {
    ExtrafieldDefinition.map(function(d, index) {
      data[d] = ExtrafieldDescription[index];
    });
  } else if (ExtrafieldDefinition && ExtrafieldDescription && ExtrafieldDefinition.length) {
    data[ExtrafieldDefinition] = ExtrafieldDescription
  }

  MongoClient.connect(config.dburl, function(err, db) {
    db.collection(config.collection)
      .insertOne(data)
      .then(function(result) {
        db.close();
        res.status(200).redirect('/');
      })
      .catch(function(err) {
        db.close();
        res.status(500).send(err.message);
      })
  });
};

module.exports.deleteData = function(req, res) {
  MongoClient.connect(config.dburl, function(err, db) {
    db.collection(config.collection)
      .findOneAndDelete({_id: new ObjectID(req.body.id)})
      .then(function(result) {
        db.close();
        res.status(200).redirect('/');
      })
      .catch(function(err) {
        db.close();
        res.status(500).send(err.message);
      })
  });
};

module.exports.updateData = function(req, res) {
  var body = Object.assign({}, req.body);
  delete body.id;

  MongoClient.connect(config.dburl, function(err, db) {
    db.collection(config.collection)
      .updateOne({_id: new ObjectID(req.body.id)}, {$set: body})
      .then(function(result) {
        setTimeout(function() {
          db.close();
          res.status(200).send('true');
        }, 1000);
      })
      .catch(function(err) {
        db.close();
        res.status(500).send(err.message);
      })
  });
}
