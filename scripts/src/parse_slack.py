# import imaplib
# import email
# from email.header import decode_header
# import requests
# import json

# # Gmail settings
# mail_server = 'imap.gmail.com'
# mail_username = 'salma@seed.com'
# mail_password = 'smpstncnjmfnpsxs'

# # Slack webhook URL
# slack_webhook_url = 'https://hooks.slack.com/services/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'

# # Connect to the mailbox
# mail = imaplib.IMAP4_SSL(mail_server)
# mail.login(mail_username, mail_password)
# mail.select("inbox")

# # Search for emails with SNS notification subject
# status, messages = mail.search(None, '(SUBJECT "AWS Notification Message")')

# for mail_id in messages[0].split():
#     _, msg_data = mail.fetch(mail_id, '(RFC822)')
#     raw_email = msg_data[0][1]

#     # Parse email content
#     msg = email.message_from_bytes(raw_email)
#     subject, encoding = decode_header(msg["Subject"])[0]
#     if isinstance(subject, bytes):
#         subject = subject.decode(encoding or "utf-8")

#     # Extract relevant information from the email
#     # Customize this based on the structure of your SNS notification emails
#     task_name = "Extracted Task Name"
#     timestamp = "Extracted Timestamp"
#     error_message = "Extracted Error Message"

#     # Format message for Slack
#     slack_message = f"Task '{task_name}' failed at {timestamp}. Error: {error_message}"

#     # Send message to Slack
#     payload = {'text': slack_message}
#     response = requests.post(slack_webhook_url, json=payload)

#     # Check if the request was successful
#     if response.status_code == 200:
#         print("Message sent to Slack successfully.")
#     else:
#         print(f"Failed to send message to Slack. Status code: {response.status_code}, Response: {response.text}")

# # Close the mailbox
# mail.logout()