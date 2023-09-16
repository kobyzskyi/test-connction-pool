const { EventEmitter } = require('events')

const pooledDownload = async (connect, save, downloadList, maxConcurrency) => {
    const connectionPool = new ConnectionPool(connect, maxConcurrency)

    await connectionPool.init()

    try {
        const downloadQueue = downloadList.map(async (file) => {
            const connection = await connectionPool.connect()

            const fileContents = await connection.download(file)

            await save(fileContents)
        })

        await Promise.all(downloadQueue)
    } finally {
        await connectionPool.close()
    }
}

class ConnectionWrapper extends EventEmitter {
    constructor({ download, close }) {
        super();
        this._download = download
        this._close = close
    }

    async download(url) {
        this.emit('download')

        const fileContents = await this._download(url)

        this.emit('downloaded')

        return fileContents
    }

    async close() {
        this.emit('close')

        await this._close()
    }
}

class ConnectionPool {
    constructor(connect, maxConcurrency) {
        this._connect = connect
        this._maxConcurrency = maxConcurrency
        this._currentConcurrency = 0
        this._connections = []
        this._queue = []
    }

    async init() {
        for (let i = 0; i < this._maxConcurrency; i++) {
            await this._createConnectionWrapper()
        }

        if (this._connections.length === 0) {
            throw new Error('connection failed')
        }
    }

    async connect() {
        if (this._currentConcurrency < this._maxConcurrency) {
            return this._connections[this._currentConcurrency++]
        }

        return this._toQueue()
    }

    _toQueue() {
        return new Promise((resolve) => {
            this._queue.push(resolve)
        })
    }

    async close() {
        await Promise.all(this._connections.map((connection) => connection.close()))
    }

    async _createConnectionWrapper() {
        try {
            const connection = await this._connect()

            const connectionWrapper = new ConnectionWrapper(connection)

            this._connections.push(connectionWrapper)

            connectionWrapper.on('downloaded', async () => {
                const next = this._queue.shift()

                if (next) {
                    next(connectionWrapper)
                }
            })

            return connectionWrapper
        } catch (e) {
            this._maxConcurrency = this._connections.length
        }
    }
}

module.exports = pooledDownload
