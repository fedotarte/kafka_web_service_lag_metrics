#kafka consumer offset and lag exporter*

- at first clone the project 


``git clone https://github.com/fedotarte/kafka_web_service_lag_metrics.git``

- then create venv

``code to create venv``


 - then activate venv

`source /venv/bin/activate`

so now you see (venv) at the left of terminal window
type this

`pip install -r requirements`

then run the project

`python app.py`

and check the response by

`localhost:5000/metrics`

