#pip install google-cloud-pubsub
#producer

import csv
import time
from google.cloud import pubsub_v1
import os

service_account_key = r"C:\Users\ACT\Documents\Python\Python-Apache-Beam\curso-dataflow-beam-323618-d9856f5d142f.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

topico = 'projects/curso-dataflow-beam-323618/topics/MeusVoos'
publisher = pubsub_v1.PublisherClient()

entrada = r"C:\Users\ACT\Documents\Python\Python-Apache-Beam\Apache-Beam\voos_sample.csv"     

with open(entrada, 'rb') as file:
    for row in file:
        print('Publishing in Topic')
        publisher.publish(topico,row)
        time.sleep(2)