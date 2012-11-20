
var fort = require('fort');
var lineReader = require('line-reader');
var events = require('events');
var util = require('util');

var GO = function(callback) {
  console.log(':: build merchant and affiliate membership indexes');
  // ADD MEMBERSHIPS
  // we are reading from a static file of memberships
  // in the real world we would be notified of membership
  // events and trigger processing by way of an event queue
  lineReader.eachLine(process.argv[2], function(line) {
    var row  = line.split("\t");
    var merchant = new Merchant(row[0]);
    row[1].split(" ").forEach(function(affiliateId) {
      merchant.add(affiliateId));
    });
  }).then(function () {
    console.log('   indexed ' + Merchant.storage.count + ' merchants');
    console.log('   indexed ' + Affiliate.storage.count + ' affiliates');
    callback();
  });
}

// STATIC COLLECTION OBJECTS
// if this was a smarter program we would use
// nosql storage instead of in memory objects
// so we could distribute work across many workers
// This example code uses this collection object
// which could be refactored to use nosql
var Collection = function() {
  this.collection = {};
  this.count = 0;
};
Collection.prototype.update = function(item) {
  if (this.collection[item.id] == undefined) {
    this.count++;   
  }
  this.collection[item.id] = item;
  return item;
};
Collection.prototype.hydrate = function(item) {
  if (this.collection[item.id] == undefined) {
    return item;
  } else { 
    return this.collection[item.id];
  }
};
Collection.prototype.get = function(id) {
  if (this.collection[id] == undefined) {
    return undefined;
  } else { 
    return this.collection[id];
  }
};

// INTERSECTIONS
var Intersections = function(id) {
  this.id = id;
  this.count = 0;
  this.storage = new Collection();
};
Intersections.prototype.get = function(id) {
  if (!(intersection = this.storage.get(id))) {
    var intersection = this.storage.update(new Intersections(id));
  }
  return intersection;
}
Intersections.prototype.index = function(a, b, item) {
  if (a != b) {
    var low, high;
    if (a > b) {
      low = b;
      high = a ;
    } else {
      low = a;
      high = b ;
    }
    intersection =  this.get(low).get(high);
    if (item != undefined) {
      this.count++;
      intersection.storage.update(item);
    }
    return intersection;
  }
}

// MODELS
var Merchant = function(id) {
  this.id = id;
  this.affiliates = new Collection();
  Merchant.storage.update(this);
};
Merchant.storage = new Collection();
Merchant.intersections = new Intersections();
Merchant.prototype.add = function(affiliate) {
  this.affiliates.update(affiliate);
  affiliate.merchants.update(this);
};
Merchant.prototype.intersect = function() {
 for (var affiliateId in this.affiliates.collection) {
   var affiliate = Affiliate.storage.get(affiliateId);
   for (var otherId in affiliate.merchants.collection) {
     Merchant.intersections.index(this.id, otherId, affiliate);
   }
 }
}

Merchant.score = function(a, b) {
  // Tanimoto Co-efficient
  na = Merchant.storage.get(a).count;
  nb = Merchant.storage.get(b).count;
  nab = Merchant.intersections.index(a,b).count;
  return nab / (na + nb - nab);
}
var Affiliate = function(id) {
  this.id = id;
  this.merchants = new Collection();
  Affiliate.storage.update(this);
};
Affiliate.storage = new Collection();

// RECOMMENDER
Recommender = function() {};
Recommender.prototype.storage = Collection();

// use a local event queue for example, but really this should
// be distributed to worker processes by way of RabbitMQ, 0MQ,
// or something similar
util.inherits(Recommender, events.EventEmitter);

// START
// Instantiate Recommendation Engine
// and kick off processing
GO(function() {
  merchant = Merchant.storage.get(111);
  merchant.intersect();
  console.log('   indexed ' + Merchant.intersections.count + ' intersections');
  console.log(Merchant.intersections.storage.collection[111].storage.collection[411]);
});

