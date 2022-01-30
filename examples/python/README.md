## Flux Realtime Python Client

This is the Flux Realtime example Python client. This client captures
data from the realtime data stream and stores it into a local json file
to be further processed.

### To Set up the Client
Create a virtualenv:
```
$ python3 -m venv .venv
```
After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.
```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:
```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.
```
$ pip install -r requirements.txt
```

Execute the client
```
$ python3 realtime-client.py -c <client-id> -s <client-secret> -o <company-name> -t TRIM_HORIZON
```
