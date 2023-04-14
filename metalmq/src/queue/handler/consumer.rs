// TODO
// modeling consumer
// - state of the consumer, init phase, consuming, cancel phase, leaving
// - limiting the number of in flight messages (qos)
// - rejecting messages - please don't send that to me again
// - anything to do with active and passive consumers?
// - identification (ip address, port, user name, vhost)
//   - maybe the server should manage client connections and consumers should just refer to those
//   connections in order to have the proper metadata for this
//
// not sure if this should be inside the queue handler but consumers are consuming queues so maybe
// this is the right place for that
