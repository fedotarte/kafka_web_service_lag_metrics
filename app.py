import os
from controller_metrics.controller import app
from waitress import serve


if __name__ == '__main__':
    app.debug = True
    # app.config['DATABASE_NAME'] = 'library.db'
    # host = os.environ.get('IP', '0.0.0.0')
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0')
    # serve(app, host='0.0.0.0', port=5000)
