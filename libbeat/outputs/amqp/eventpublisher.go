package amqp

import (
	"github.com/elastic/beats/libbeat/logp"
	"github.com/streadway/amqp"
)

// newEventPublisher creates a new publisher for sending and confirming prepared
// events using the provided AMQP channel.
//
// The provided AMQP channel MUST NOT have had any previous publishes. Publishes
// will fail if the delivery tag starts at a value other than 1.
//
// The publisher will try and put the provided channel into confirm mode. An
// error will be returned if this fails.
func newEventPublisher(logger *logp.Logger, channel amqpChannel, declarer exchangeDeclarer, preparedEvents <-chan preparedEvent, pendingBufferSize uint64, mandatory, immediate bool) (*eventPublisher, error) {
	if err := channel.Confirm(false); err != nil {
		return nil, err
	}

	ep := &eventPublisher{
		logger:         logger,
		channel:        channel,
		preparedEvents: preparedEvents,
		mandatory:      mandatory,
		immediate:      immediate,
		declarer:       declarer,

		exchanges:        map[string]empty{},
		pendingChan:      make(chan pendingPublish, pendingBufferSize),
		doneChan:         make(chan error, 1),
		confirmationChan: channel.NotifyPublish(make(chan amqp.Confirmation)),
	}

	ep.logger.Debugf("eventPublisher starting")

	go ep.confirmWorker()
	go ep.publishWorker()

	return ep, nil
}

type eventPublisher struct {
	logger         *logp.Logger
	channel        amqpChannel
	preparedEvents <-chan preparedEvent
	mandatory      bool
	immediate      bool
	declarer       exchangeDeclarer

	exchanges        map[string]empty
	pendingChan      chan pendingPublish
	doneChan         chan error
	confirmationChan chan amqp.Confirmation
	deliveryTag      uint64
}

func (e *eventPublisher) done() (err error) {
	defer e.logger.Debugf("eventPublisher finished")
	for e := range e.doneChan {
		if e != nil && err == nil {
			err = e
		}
	}
	return
}

func (e *eventPublisher) ensureDeclared(exchange string) error {
	if _, declared := e.exchanges[exchange]; !declared {
		if err := e.declarer(e.channel, exchange); err != nil {
			return err
		}
		e.exchanges[exchange] = empty{}
	}
	return nil
}

// TODO: might need to handle returns unless they also get sent to the confirm channel, in which case we may at least want to log them
//
//func (c *client) handleReturns(returns <-chan amqp.Return) {
//	defer c.closeWaitGroup.Done()
//	defer c.logger.Debugf("returns worker finished")
//	c.logger.Debugf("returns worker starting")
//
//	for ret := range returns {
//		c.logger.Debugf("return received: %#v", ret) // TODO: check what to log here
//	}
//}

func (e *eventPublisher) confirmWorker() {
	defer close(e.doneChan)
	defer e.logger.Debugf("confirmWorker finished")
	e.logger.Debugf("confirmWorker starting")

	var warned bool
	for pending := range e.pendingChan {
		confirmation, open := <-e.confirmationChan
		if !open && !warned {
			// Just warn once to avoid log spam in case the confirm channel
			// closes when there are many pending publishes.
			warned = true
			e.logger.Warnf("AMQP confirmation channel closed early! all pending publishes will be considered NACKed and retried")
		}

		var deliveryMatch bool
		if open {
			// The AMQP library is meant to guarantee that confirmation order
			// matches publish order. If there's a mismatch here, then either
			// that guarantee has been broken, or we've messed up somehow.
			deliveryMatch = confirmation.DeliveryTag == pending.deliveryTag
			if !deliveryMatch {
				e.logger.Errorf("AMQP confirmation delivery tag (%v) does not match pending delivery tag (%v)! pending publish will be considered NACKed and retried", confirmation.DeliveryTag, pending.deliveryTag)
			}
		}

		if open && deliveryMatch && confirmation.Ack {
			pending.event.incomingEvent.batchTracker.confirmEvent()
		} else {
			pending.event.incomingEvent.batchTracker.retryEvent(pending.event.incomingEvent.event)
		}
	}
}

// publishWorker attempts to publish the contents of preparedEvents to the
// current AMQP channel.
//
// publishWorker ends when preparedEvents is closed, when declaring an exchange
// fails, or when channel.Publish fails.
func (e *eventPublisher) publishWorker() {
	// Note: Let confirmWorker close doneChan, which it should do shortly after
	// pendingChan is closed.
	defer close(e.pendingChan)
	defer e.logger.Debugf("publishWorker finished")
	e.logger.Debugf("publishWorker starting")

	for event := range e.preparedEvents {
		if err := e.ensureDeclared(event.exchangeName); err != nil {
			// Errors returned from exchange declare are meant to close the AMQP
			// channel, so we shut down the publisher since we won't be able to
			// publish future events.
			event.incomingEvent.batchTracker.retryEvent(event.incomingEvent.event)
			e.logger.Errorf("AMQP exchange declare error: %v", err)
			e.doneChan <- err
			return
		}

		if err := e.channel.Publish(
			event.exchangeName,
			event.routingKey,
			e.mandatory,
			e.immediate,
			event.outgoingPublishing,
		); err != nil {
			// Publish is asynchronous, so the assumption here is that a Publish
			// error is a result of a connection issue. So we shut down the
			// publisher, but only after signalling to the batch that the event
			// should be retried (since it's no longer on the preparedEvents
			// channel).
			event.incomingEvent.batchTracker.retryEvent(event.incomingEvent.event)
			e.logger.Errorf("AMQP publish error: %v", err)
			e.doneChan <- err
			return
		}

		e.deliveryTag++
		e.pendingChan <- pendingPublish{
			event:       event,
			deliveryTag: e.deliveryTag,
		}
	}
}