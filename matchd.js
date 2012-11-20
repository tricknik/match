var lineReader = require('line-reader');
var util = require('util');

// if this was a smarter program we would use
// nosql storage instead of in memory objects
// so we could distribute work across many workers
var merchants = {};
var affiliates = {};

// build an intersection index
var icount = 0;
var intersections = {};
var intersect = function(a, b, affiliate) {
  if (a != b) {
    var low, high;
    if (a > b) {
      low = b;
      high = a ;
    } else {
      low = a;
      high = b ;
    }
    if (intersections[low] == undefined) {
      intersections[low] = {};
    }
    if (intersections[low][high] == undefined) {
      intersections[low][high] = [];
      icount++;
    }
    if (affiliate == undefined) {
      return intersections[low][high]
    } else {
      if (intersections[low][high].indexOf(affiliate) == -1) {
        intersections[low][high].push(affiliate);
      } 
    }
  }
};

// we are reading from a static file of memberships
// in the real world we would be notified of membership
// events and trigger processing by way of an event queue
console.log(':: build merchant and affiliate membership indexes');
var mcount = 0;
var acount = 0;
lineReader.eachLine(process.argv[2], function(line) {
  var row  = line.split("\t");
  merchants[row[0]] =  row[1].split(" ");
  merchants[row[0]].forEach(function(affiliate) {
    if (affiliates[affiliate] == undefined) {
      affiliates[affiliate] =  []; 
      acount++;
    }
    affiliates[affiliate].push(row[0]);
  });
  mcount++;
}).then(function () {
  console.log('   indexed ' + mcount + ' merchants');
  console.log('   indexed ' + acount + ' affiliates');
  console.log(':: build merchant intersection index');
  for (var merchant in merchants) {
    merchants[merchant].forEach(function(affiliate) {
      affiliates[affiliate].forEach(function(other) {
        intersect(merchant, other, affiliate);
      });
    });
  }
  console.log('   indexed ' + icount + ' intersections');

  // SOCKET
  // Listen on socket 9999 for connection from client
  var io = require('socket.io').listen(9999);
  io.sockets.on('connection', function(socket) {
    console.log('connection!');
    socket.on('add', function() {
      console.log('add');
    });
  });

  console.log('listening...');
});
