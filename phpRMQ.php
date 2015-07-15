<?php

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . 'amqConfig.php';
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMqAdapter{


	/**
		AMQ (RabbitMQ) Message Sending

		@author undertuga <undertuga@tutanota.de>
		@param Array $amqData Array with message and queue data
		@throw Exception Exception on param|connection error.
		@return Boolean True on message sending success, False on message sending failure

	*/
	public function SendMessage($amqData){

		// implementing some error control scheme
		try{

			// validating gathered params
			if(empty($amqData) || !isset($amqData['queue']) || !isset($amqData['message']))
				throw new Exception("Invalid given params", 1);
				
			// declaring new RabbitMQ connection
			$connection = new AMQPConnection(constant('host'), constant('port'), constant('user'), constant('pass'));

			// validating connection
			if(!$connection)
				throw new Exception("Error while connecting to RabbitMQ", 1);

			// declaring new channel to send message
			$channel = $connection->channel();

			// preparing AMQ message to send
			$msg = new AMQPMessage(json_encode($amqData['message']));

			// try to publish given message to desired queue
			if($channel->basic_publish($msg, '', $amqData['queue'])){

				// clean up and bail out with success
				$channel->close();
				$connection->close();
				return true;

			} else {

				// fail safe bail out
				$channel->close();
				$connection->close();
				throw new Exception("Error while publishing message @ " . amqData['queue'], 1);
				
			}

		} catch(Exception $error){
			return "Error while sending AMQ message: ".$error->getError();
		}
	}



	/**
		AMQ (RabbitMQ) Message Sending

		@author undertuga <undertuga@tutanota.de>
		@param Array $amqData Array with message and queue data
		@throw Exception Exception on param|connection error.
		@return Boolean True on message sending success, False on message sending failure

	*/
	public function GetMessages($amqData){

		// implementing some error control scheme
		try{

			// validating gathered params
			if(empty($amqData) || !isset($amqData['queue']))
				throw new Exception("Invalid given params", 1);

			// declaring new RabbitMQ connection
			$connection = new AMQPConnection(constant('host'), constant('port'), constant('user'), constant('pass'));

			// validating connection
			if(!$connection)
				throw new Exception("Error while connecting to RabbitMQ", 1);

			// declaring channel & queue to get message from
			$channel = $connection->channel();
			$channel->queue_declare($amqData['queue'], false, false, false, false);

			// declaring message receive callback
			$callback = function($message){
				
				// validating gathered message object
				if(empty($message))
					return false;

				// return gathered message
				return $message->body;
			}

			// consuming queue messages
			$channel->basic_consume($amqData['queue'], '', false, true, false, false, $callback);

			// looping messages
			while(count($channel->callbacks)){
			    $channel->wait();
			}

			// clean up
			$channel->close();
			$connection->close();
	
		} catch(Exception $error){
			return "Error while sending AMQ message: ".$error->getError();
		}
	}
}