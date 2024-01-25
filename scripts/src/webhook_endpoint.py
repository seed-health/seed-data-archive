from flask import Flask, request, jsonify
import requests
import imaplib
import email
from email.header import decode_header

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    try:
        # Extract email content from the request
        data = request.get_json()

        # Assuming the email content is in the 'email_content' field
        email_content = data.get('email_content')

        if email_content:
            # Parse email content
            msg = email.message_from_string(email_content)

            # Extract relevant information from the email
            subject, encoding = decode_header(msg["Subject"])[0]
            if isinstance(subject, bytes):
                subject = subject.decode(encoding or "utf-8")

            # Extract the body of the email
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True)
                        break
            else:
                body = msg.get_payload(decode=True)

            # Customize this based on the structure of forwarded emails
            task_name = subject
            timestamp = "Extracted Timestamp"  # You need to extract this from the body or other headers
            error_message = body.decode("utf-8")  # Assuming the error message is in the email body

            # Format message for Slack
            slack_message = f"Task '{task_name}' failed at {timestamp}. Error: {error_message}"

            # Send message to Slack
            slack_webhook_url = 'https://hooks.slack.com/services/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'
            payload = {'text': slack_message}
            response = requests.post(slack_webhook_url, json=payload)

            # Check if the request was successful
            if response.status_code == 200:
                print("Message sent to Slack successfully.")
            else:
                print(f"Failed to send message to Slack. Status code: {response.status_code}, Response: {response.text}")
        else:
            print("No email content found in the request.")

    except Exception as e:
        print(f"Error handling webhook: {str(e)}")

    return jsonify({'status': 'OK'})

if __name__ == '__main__':
    app.run(port=5000)
