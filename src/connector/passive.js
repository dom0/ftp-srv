const net = require('net');
const tls = require('tls');
const ip = require('ip');
const Promise = require('bluebird');

const Connector = require('./base');
const errors = require('../errors');

const CONNECT_TIMEOUT = 30 * 1000;

class Passive extends Connector {
  constructor(connection) {
    super(connection);
    this.type = 'passive';

    // ✅ FIX: latch per non perdere la connessione "troppo veloce"
    this._pendingSocket = null;
    this._pendingResolve = null;
  }

  waitForConnection({timeout = 5000, delay = 50} = {}) {
    if (!this.dataServer) return Promise.reject(new errors.ConnectorError('Passive server not setup'));

    // ✅ FIX: se la connessione è già arrivata e passata "in un lampo"
    if (this._pendingSocket) {
      const s = this._pendingSocket;
      this._pendingSocket = null;
      return Promise.resolve(s);
    }

    const checkSocket = () => {
      // ✅ FIX: considera anche il latch (non solo polling su connected)
      if (this._pendingSocket) {
        const s = this._pendingSocket;
        this._pendingSocket = null;
        return Promise.resolve(s);
      }

      if (this.dataServer && this.dataServer.listening && this.dataSocket && this.dataSocket.connected) {
        return Promise.resolve(this.dataSocket);
      }

      return Promise.resolve()
        .delay(delay)
        .then(() => checkSocket());
    };

    // ✅ FIX: oltre al polling, aspetta anche l'evento (race)
    const eventPromise = new Promise((resolve) => {
      this._pendingResolve = resolve;
    });

    return Promise.race([eventPromise, checkSocket()])
      .timeout(timeout)
      .finally(() => {
        // cleanup: evita di lasciare resolve appeso tra comandi
        this._pendingResolve = null;
      });
  }

  setupServer() {
    this.closeServer();
    return this.server.getNextPasvPort()
    .then((port) => {
      this.dataSocket = null;
      this._pendingSocket = null;
      this._pendingResolve = null;

      let idleServerTimeout;

      const fulfill = (socket) => {
        this._pendingSocket = socket;
        if (this._pendingResolve) {
          const r = this._pendingResolve;
          this._pendingResolve = null;
          r(socket);
        }
      };

      const connectionHandler = (socket) => {
        if (!ip.isEqual(this.connection.commandSocket.remoteAddress, socket.remoteAddress)) {
          this.log.error({
            pasv_connection: socket.remoteAddress,
            cmd_connection: this.connection.commandSocket.remoteAddress
          }, 'Connecting addresses do not match');

          socket.destroy();
          return this.connection.reply(550, 'Remote addresses do not match')
          .then(() => this.connection.close());
        }
        clearTimeout(idleServerTimeout);

        this.log.trace({port, remoteAddress: socket.remoteAddress}, 'Passive connection fulfilled.');

        this.dataSocket = socket;
        this.dataSocket.on('error', (err) => this.server && this.server.emit('client-error', {connection: this.connection, context: 'dataSocket', error: err}));
        this.dataSocket.once('close', () => this.closeServer());

        // ✅ FIX: cattura subito la connessione (anche se poi dura pochissimo)
        fulfill(socket);

        if (!this.connection.secure) {
          this.dataSocket.connected = true;
        }
      };

      const serverOptions = Object.assign({}, this.connection.secure ? this.server.options.tls : {}, {pauseOnConnect: true});
      this.dataServer = (this.connection.secure ? tls : net).createServer(serverOptions, connectionHandler);
      this.dataServer.maxConnections = 1;

      this.dataServer.on('error', (err) => this.server && this.server.emit('client-error', {connection: this.connection, context: 'dataServer', error: err}));
      this.dataServer.once('close', () => {
        this.log.trace('Passive server closed');
        this.end();
      });

      if (this.connection.secure) {
        this.dataServer.on('secureConnection', (socket) => {
          socket.connected = true;

          // ✅ FIX: se arriva già TLS-ready, risolvi anche qui
          fulfill(socket);
        });
      }

      return new Promise((resolve, reject) => {
        this.dataServer.listen(port, this.server.url.hostname, (err) => {
          if (err) reject(err);
          else {
            idleServerTimeout = setTimeout(() => this.closeServer(), CONNECT_TIMEOUT);

            this.log.debug({port}, 'Passive connection listening');
            resolve(this.dataServer);
          }
        });
      });
    })
    .catch((error) => {
      this.log.trace(error.message);
      throw error;
    });
  }

}
module.exports = Passive;
