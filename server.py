from flask import Flask
import etcd3
import threading
import os
import time

app = Flask(__name__)

# Etcd client
client = etcd3.client()

# Server port
PORT = os.environ.get('PORT', 5000)

# Leader key in etcd
LEADER_KEY = '/leader'

def elect_leader():
    while True:
        # Try to create the leader key with this server's port as the value
        # client.put(LEADER_KEY, str(PORT), lease=client.lease(10))
        with client.transaction() as txn:
            is_key_present = client.transactions.get(LEADER_KEY) != (None, None)

            if not is_key_present:
                txn.success(client.transactions.put(LEADER_KEY, str(PORT)), lease=client.lease(10))
            else:
                print('Not the leader')
                txn.success()  # Or handle the case where the key exists as needed

            # Commit the transaction
            committed, responses = txn.commit()

        # Check if the transaction was committed successfully
        if committed:
            print("Transaction committed successfully.")
            if not is_key_present:
                print(f"there is already a leader: '{str(PORT)}'")
            else:
                print(f"there is no leader, so I am the leader: '{str(PORT)}'")
        else:
            print("Transaction failed, retry in the next check.")
        time.sleep(5)

@app.route('/hello')
def hello():
    try:
        leader_port = client.get(LEADER_KEY)[0].decode('utf-8')
        if leader_port == str(PORT):
            return 'hello world'
        else:
            return 'Leader is running on port {}'.format(leader_port)
    except Exception as e:
        return 'Leader not found: {}'.format(e)

if __name__ == '__main__':
    # Start the leader election process
    threading.Thread(target=elect_leader).start()
    # Start the Flask server
    app.run(port=PORT)
