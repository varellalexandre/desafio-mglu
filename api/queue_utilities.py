import boto3
def format_json(
	body,
	request_datetime,
	remote_address
):
	formatted_message = dict()
	formatted_message['body']={
		'DataType':'String',
		'StringValue':body
	}
	formatted_message['request_datetime']={
		'DataType':'String',
		'StringValue':request_datetime
	}
	formatted_message['remote_address']={
		'DataType':'String',
		'StringValue':remote_address
	}
	return formatted_message

def send_info_to_queue(
    topic_arn:str,
    titulo:str,
    attributes:dict,
    message:dict
):
	client = boto3.client('sns')
	client.publish(
		TopicArn=topic_arn,
		Message=message,
		Subject=titulo,
		MessageAttributes=attributes
	)
