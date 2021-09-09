# consumer

import csv
import time
from google.cloud import pubsub_v1
import os


service_account_key = r'C:\Users\ACT\Documents\Python\Python-Apache-Beam\curso-dataflow-beam-323618-d9856f5d142f.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

subscription = 'projects/curso-dataflow-beam-323618/subscriptions/MeusVoos-Sub'
subscriber = pubsub_v1.SubscriberClient()

def monstrar_msg(mensagem):
  print(('Mensagem: {}'.format(mensagem)))
  mensagem.ack()

subscriber.subscribe(subscription,callback=monstrar_msg)

while True:
  time.sleep(3)