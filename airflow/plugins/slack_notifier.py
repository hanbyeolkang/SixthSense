"""
Slack 알림 모듈
Airflow Task 실패/성공 시 Slack으로 알림 전송
"""

import requests
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Slack Webhook을 통한 알림 전송"""
    
    def __init__(self, webhook_url: str):
        """
        Args:
            webhook_url: Slack Incoming Webhook URL
        """
        self.webhook_url = webhook_url

    
    # def send_message_to_a_slack_channel(message, emoji, channel, access_token):
    def send_message_to_a_slack_channel(message, emoji):
        # url = "https://slack.com/api/chat.postMessage"
        url = "https://hooks.slack.com/services/" + Variable.get("slack_url")
        headers = {
            'content-type': 'application/json',
        }
        data = { "username": Variable.get("username"), "text": message, "icon_emoji": emoji }
        r = requests.post(url, json=data, headers=headers)
        return r
    
    
    def on_failure_callback(context):
        """
        https://airflow.apache.org/_modules/airflow/operators/slack_operator.html
        Define the callback to post on Slack if a failure is detected in the Workflow
        :return: operator.execute
        """
        text = str(context['task_instance'])
        text += "```" + str(context.get('exception')) +"```"
        send_message_to_a_slack_channel(text, ":scream:")

