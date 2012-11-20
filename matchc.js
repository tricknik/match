var io = require('socket.io-client');
var socket = io.connect('localhost', {port: 9999});
socket.on('connect', function() {
  console.log('hey hey hey');
  socket.emit('add', [1, 2]);
  socket.disconnect();
});
