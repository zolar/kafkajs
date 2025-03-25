const Address6 = require('ip-address').Address6

const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../errors')
const ConnectionPool = require('../network/connectionPool')

/**
 * @typedef {Object} ConnectionPoolBuilder
 * @property {(destination?: { host?: string, port?: number, rack?: string }) => Promise<ConnectionPool>} build
 */

/**
 * @param {Object} options
 * @param {import("../../types").ISocketFactory} [options.socketFactory]
 * @param {string[]|(() => string[])} options.brokers
 * @param {Object} [options.ssl]
 * @param {Object} [options.sasl]
 * @param {string} options.clientId
 * @param {number} options.requestTimeout
 * @param {boolean} [options.enforceRequestTimeout]
 * @param {number} [options.connectionTimeout]
 * @param {number} [options.maxInFlightRequests]
 * @param {import("../../types").RetryOptions} [options.retry]
 * @param {import("../../types").Logger} options.logger
 * @param {import("../instrumentation/emitter")} [options.instrumentationEmitter]
 * @param {number} [options.reauthenticationThreshold]
 * @returns {ConnectionPoolBuilder}
 */
module.exports = ({
  socketFactory,
  brokers,
  ssl,
  sasl,
  clientId,
  requestTimeout,
  enforceRequestTimeout,
  connectionTimeout,
  maxInFlightRequests,
  logger,
  instrumentationEmitter = null,
  reauthenticationThreshold,
}) => {
  let index = 0

  const isValidBroker = broker => {
    return broker && typeof broker === 'string' && broker.length > 0
  }

  const validateBrokers = brokers => {
    if (!brokers) {
      throw new KafkaJSNonRetriableError(`Failed to connect: brokers should not be null`)
    }

    if (Array.isArray(brokers)) {
      if (!brokers.length) {
        throw new KafkaJSNonRetriableError(`Failed to connect: brokers array is empty`)
      }

      brokers.forEach((broker, index) => {
        if (!isValidBroker(broker)) {
          throw new KafkaJSNonRetriableError(
            `Failed to connect: broker at index ${index} is invalid "${typeof broker}"`
          )
        }
      })
    }
  }

  const parseIPv6 = address => {
    try {
      const address6 = new Address6(address)

      logger.debug(`parseIPv6.address6.isValid => ${address6.isValid()}`)
      logger.debug(
        `parseIPv6.address6.correctForm => ${address6.isValid() ? address6.correctForm() : ''}`
      )

      return address6.isValid() ? address6.correctForm() : null
    } catch (error) {
      return null
    }
  }

  const getBrokers = async () => {
    let list

    if (typeof brokers === 'function') {
      try {
        list = await brokers()
      } catch (e) {
        const wrappedError = new KafkaJSConnectionError(
          `Failed to connect: "config.brokers" threw: ${e.message}`
        )
        wrappedError.stack = `${wrappedError.name}\n  Caused by: ${e.stack}`
        throw wrappedError
      }
    } else {
      list = brokers
    }

    validateBrokers(list)

    return list
  }

  return {
    build: async ({ host, port, rack } = {}) => {
      if (!host) {
        const list = await getBrokers()
        const randomBroker = list[index++ % list.length]

        host = randomBroker.split(':')[0]

        logger.debug(`connectionPoolBuilder.randomBroker => ${randomBroker}`)
        parseIPv6(randomBroker)
        logger.debug(`connectionPoolBuilder.host => ${host}`)
        parseIPv6(host)
        logger.debug(`connectionPoolBuilder.port => ${port}`)

        port = Number(randomBroker.split(':')[1])
      }

      return new ConnectionPool({
        host,
        port,
        rack,
        sasl,
        ssl,
        clientId,
        socketFactory,
        connectionTimeout,
        requestTimeout,
        enforceRequestTimeout,
        maxInFlightRequests,
        instrumentationEmitter,
        logger,
        reauthenticationThreshold,
      })
    },
  }
}
