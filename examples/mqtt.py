#!/usr/bin/python3

# original script by Tom Fojta
# https://fojta.wordpress.com/2020/04/22/vmware-cloud-director-push-notifications-in-tenant-context/

import paho.mqtt.client as mqtt
import json
import datetime
import pyvcloud.vcd.client
import pyvcloud.vcd.vdc

vcdHost = 'vcloud.fojta.com'
vcdPort = 443
path = "/messaging/mqtt"
logFile = 'vcd_log.log'

# org admin credentials
user = 'acmeadmin'
password = 'VMware1!'
org = 'acme'

credentials = pyvcloud.vcd.client.BasicLoginCredentials(user, org, password)
vcdClient = pyvcloud.vcd.client.Client(vcdHost+":"+str(vcdPort), None, True, logFile)
vcdClient.set_credentials(credentials)
accessToken = vcdClient.get_access_token()
headers = {"Authorization": "Bearer " + accessToken}

if max(vcdClient.get_supported_versions_list()) < "34.0":
    exit('VMware Cloud Director 10.1 or newer is required')

org = vcdClient.get_org_list()
orgId = (org[0].attrib).get('id').split('org:', 1)[1]


def on_message(client, userdata, message):
    event = message.payload.decode('utf-8')
    event = event.replace('\\', '')
    eventPayload = event.split('payload":"', 1)[1]
    eventPayload = eventPayload[:-2]
    event_json = json.loads(eventPayload)
    print(datetime.datetime.now())
    print(event_json)


# Enable for logging
# def on_log(client, userdata, level, buf):
#     print("log: ",buf)


client = mqtt.Client(client_id="PythonMQTT", transport="websockets")
client.ws_set_options(path=path, headers=headers)
client.tls_set_context(context=None)
# client.tls_insecure_set(True)
client.on_message = on_message
# client.on_log=on_log  #enable for logging
client.connect(host=vcdHost, port=vcdPort, keepalive=60)
print('Connected')
client.subscribe("publish/"+orgId+"/*")
client.loop_forever()
