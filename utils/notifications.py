import sys
import requests
import json

def notify_slack(message='empty',title='Slack notification',color="#F74E00"):
    url = "https://hooks.slack.com/services/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"
    slack_data = {
        "username": title, "channel" : "#data-notification",
        "attachments": [{"color": color, "fields": [{"value": message, "short": "false"}]}]   
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)